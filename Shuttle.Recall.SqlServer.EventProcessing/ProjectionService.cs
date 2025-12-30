using System.Transactions;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Threading;
using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public class ProjectionService(IOptions<EventStoreOptions> eventStoreOptions, IOptions<SqlServerEventProcessingOptions> sqlServerEventProcessingOptions, IProjectionRepository projectionRepository, IProjectionQuery projectionQuery, IPrimitiveEventQuery primitiveEventQuery, IEventProcessorConfiguration eventProcessorConfiguration)
    : IProjectionService, IPipelineObserver<ThreadPoolsStarted>
{
    private class BalancedProjection(Projection projection, IEnumerable<TimeSpan> backoffDurations)
    {
        private readonly TimeSpan[] _durations = Guard.AgainstEmpty(backoffDurations).ToArray();
        private int _durationIndex;

        public Projection Projection { get; } = projection;
        public DateTimeOffset IgnoreTillDateTime { get; private set; } = DateTimeOffset.MinValue;

        public void Idle()
        {
            if (_durationIndex >= _durations.Length)
            {
                _durationIndex = _durations.Length - 1;
            }

            IgnoreTillDateTime = DateTimeOffset.UtcNow.Add(_durations[_durationIndex++]);
        }

        public void Resume()
        {
            IgnoreTillDateTime = DateTimeOffset.MinValue;
            _durationIndex = 0;
        }
    }

    private readonly EventStoreOptions _eventStoreOptions = Guard.AgainstNull(Guard.AgainstNull(eventStoreOptions).Value);
    private readonly SqlServerEventProcessingOptions _sqlServerEventProcessingOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerEventProcessingOptions).Value);
    private readonly IEventProcessorConfiguration _eventProcessorConfiguration = Guard.AgainstNull(eventProcessorConfiguration);
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly IPrimitiveEventQuery _primitiveEventQuery = Guard.AgainstNull(primitiveEventQuery);
    private readonly IProjectionQuery _projectionQuery = Guard.AgainstNull(projectionQuery);
    private readonly IProjectionRepository _projectionRepository = Guard.AgainstNull(projectionRepository);
    private readonly Dictionary<string, List<ThreadPrimitiveEvent>> _projectionThreadPrimitiveEvents = new();
    private int[] _managedThreadIds = [];

    private BalancedProjection[] _balancedProjections = [];
    private int _roundRobinIndex;

    public async Task<ProjectionEvent?> RetrieveEventAsync(IPipelineContext<RetrieveEvent> pipelineContext, CancellationToken cancellationToken = default)
    {
        var processorThreadManagedThreadId = Guard.AgainstNull(pipelineContext).Pipeline.State.GetProcessorThreadManagedThreadId();

        if (_balancedProjections.Length == 0)
        {
            return null;
        }

        for (var i = 0; i < _balancedProjections.Length; i++)
        {
            BalancedProjection balancedProjection;

            await _lock.WaitAsync(cancellationToken);
            try
            {
                var currentIndex = (_roundRobinIndex + i) % _balancedProjections.Length;
                balancedProjection = _balancedProjections[currentIndex];
                _roundRobinIndex = (currentIndex + 1) % _balancedProjections.Length;
            }
            finally
            {
                _lock.Release();
            }

            if (balancedProjection.IgnoreTillDateTime > DateTimeOffset.UtcNow)
            {
                continue;
            }

            var projectionThreadPrimitiveEvents = _projectionThreadPrimitiveEvents[balancedProjection.Projection.Name];

            if (!projectionThreadPrimitiveEvents.Any())
            {
                await GetProjectionJournalAsync(balancedProjection.Projection);
            }

            if (!projectionThreadPrimitiveEvents.Any())
            {
                balancedProjection.Idle();
                continue;
            }

            balancedProjection.Resume();

            var threadPrimitiveEvent = projectionThreadPrimitiveEvents.FirstOrDefault(item => item.ManagedThreadId == processorThreadManagedThreadId);

            if (threadPrimitiveEvent != null)
            {
                return new(balancedProjection.Projection, threadPrimitiveEvent.PrimitiveEvent);
            }
        }

        return null;
    }

    public async Task AcknowledgeEventAsync(IPipelineContext<AcknowledgeEvent> pipelineContext, CancellationToken cancellationToken = default)
    {
        var projectionEvent = Guard.AgainstNull(pipelineContext).Pipeline.State.GetProjectionEvent();

        await _projectionRepository.CompleteAsync(projectionEvent, cancellationToken);

        await _lock.WaitAsync(cancellationToken);

        try
        {
            _projectionThreadPrimitiveEvents[projectionEvent.Projection.Name].RemoveAll(item => item.PrimitiveEvent.SequenceNumber == projectionEvent.PrimitiveEvent.SequenceNumber);
        }
        finally
        {
            _lock.Release();
        }
    }

    private int GetManagedThreadIdIndex(PrimitiveEvent primitiveEvent)
    {
        return Math.Abs((primitiveEvent.CorrelationId ?? primitiveEvent.Id).GetHashCode()) % _managedThreadIds.Length;
    }

    private async Task GetProjectionJournalAsync(Projection projection)
    {
        await _lock.WaitAsync();

        try
        {
            if (_projectionThreadPrimitiveEvents[projection.Name].Any())
            {
                return;
            }

            var journalSequenceNumbers = new List<long>();

            using (new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled))
            {
                var specification = new PrimitiveEvent.Specification()
                    .WithMaximumRows(_sqlServerEventProcessingOptions.ProjectionBatchSize)
                    .WithSequenceNumberStart(projection.SequenceNumber + 1)
                    .WithSequenceNumberEnd(projection.SequenceNumber + _sqlServerEventProcessingOptions.ProjectionBatchSize);

                foreach (var primitiveEvent in (await _primitiveEventQuery.SearchAsync(specification)).OrderBy(item => item.SequenceNumber))
                {
                    var managedThreadId = _managedThreadIds[GetManagedThreadIdIndex(primitiveEvent)];

                    _projectionThreadPrimitiveEvents[projection.Name].Add(new(managedThreadId, primitiveEvent));

                    journalSequenceNumbers.Add(primitiveEvent.SequenceNumber!.Value);
                }
            }

            await _projectionRepository.RegisterJournalSequenceNumbersAsync(projection.Name, journalSequenceNumbers).ConfigureAwait(false);

            if (journalSequenceNumbers.Any())
            {
                await _projectionRepository.SetSequenceNumberAsync(projection.Name, journalSequenceNumbers.Max());
            }
        }
        finally
        {
            _lock.Release();
        }
    }
    
    public async Task ExecuteAsync(IPipelineContext<ThreadPoolsStarted> pipelineContext, CancellationToken cancellationToken = default)
    {
        var processorThreadPool = Guard.AgainstNull(Guard.AgainstNull(pipelineContext).Pipeline.State.Get<IProcessorThreadPool>("ProjectionProcessorThreadPool"));

        _managedThreadIds = processorThreadPool.ProcessorThreads.Select(item => item.ManagedThreadId).ToArray();

        Dictionary<string, List<long>> incompleteSequenceNumbers = new();

        List<BalancedProjection> balancedProjections = [];

        foreach (var projectionConfiguration in _eventProcessorConfiguration.Projections)
        {
            balancedProjections.Add(new(await _projectionRepository.GetAsync(projectionConfiguration.Name, cancellationToken), _eventStoreOptions.ProjectionProcessorIdleDurations));

            _projectionThreadPrimitiveEvents.Add(projectionConfiguration.Name, []);

            incompleteSequenceNumbers.Add(projectionConfiguration.Name, [.. await _projectionQuery.GetIncompleteSequenceNumbersAsync(projectionConfiguration.Name, cancellationToken)]);
        }

        _balancedProjections = balancedProjections.ToArray();

        foreach (var pair in incompleteSequenceNumbers)
        {
            if (pair.Value.Count == 0)
            {
                continue;
            }

            var specification = new PrimitiveEvent.Specification();

            specification.WithSequenceNumbers(pair.Value);

            foreach (var primitiveEvent in (await _primitiveEventQuery.SearchAsync(specification, cancellationToken)).OrderBy(item => item.SequenceNumber))
            {
                var managedThreadId = _managedThreadIds[GetManagedThreadIdIndex(primitiveEvent)];

                _projectionThreadPrimitiveEvents[pair.Key].Add(new(managedThreadId, primitiveEvent));
            }
        }
    }
}