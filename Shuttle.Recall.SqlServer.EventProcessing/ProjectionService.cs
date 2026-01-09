using System.Transactions;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Threading;
using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public class ProjectionService(IOptions<RecallOptions> recallOptions, IOptions<SqlServerEventProcessingOptions> sqlServerEventProcessingOptions, IProjectionRepository projectionRepository, IProjectionQuery projectionQuery, IPrimitiveEventQuery primitiveEventQuery, IEventProcessorConfiguration eventProcessorConfiguration)
    : IProjectionService, IPipelineObserver<ThreadPoolsStarted>
{
    private readonly List<ProjectionExecutionContext> _projectionExecutionContexts = [];
    private readonly IEventProcessorConfiguration _eventProcessorConfiguration = Guard.AgainstNull(eventProcessorConfiguration);
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly IPrimitiveEventQuery _primitiveEventQuery = Guard.AgainstNull(primitiveEventQuery);
    private readonly IProjectionQuery _projectionQuery = Guard.AgainstNull(projectionQuery);
    private readonly IProjectionRepository _projectionRepository = Guard.AgainstNull(projectionRepository);
    private readonly RecallOptions _recallOptions = Guard.AgainstNull(Guard.AgainstNull(recallOptions).Value);
    private readonly SqlServerEventProcessingOptions _sqlServerEventProcessingOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerEventProcessingOptions).Value);

    private int[] _managedThreadIds = [];
    private int _roundRobinIndex;

    public async Task ExecuteAsync(IPipelineContext<ThreadPoolsStarted> pipelineContext, CancellationToken cancellationToken = default)
    {
        var processorThreadPool = Guard.AgainstNull(Guard.AgainstNull(pipelineContext).Pipeline.State.Get<IProcessorThreadPool>("ProjectionProcessorThreadPool"));

        _managedThreadIds = processorThreadPool.ProcessorThreads.Select(item => item.ManagedThreadId).ToArray();

        if (_managedThreadIds.Length == 0)
        {
            throw new ApplicationException(Resources.ManagedThreadIdsException);
        }

        await _lock.WaitAsync(cancellationToken);

        try
        {
            foreach (var projectionConfiguration in _eventProcessorConfiguration.Projections)
            {
                var projectionExecutionContext = new ProjectionExecutionContext(await _projectionRepository.GetAsync(projectionConfiguration.Name, cancellationToken), _recallOptions.EventProcessing.ProjectionProcessorIdleDurations);

                await projectionExecutionContext.Lock.WaitAsync(cancellationToken);

                try
                {
                    var incompleteSequenceNumbers = (await _projectionQuery.GetIncompleteSequenceNumbersAsync(projectionConfiguration.Name, cancellationToken)).ToList();

                    if (incompleteSequenceNumbers.Count > 0)
                    {
                        var specification = new PrimitiveEvent.Specification().WithSequenceNumbers(incompleteSequenceNumbers);

                        foreach (var primitiveEvent in (await _primitiveEventQuery.SearchAsync(specification, cancellationToken)).OrderBy(item => item.SequenceNumber))
                        {
                            var managedThreadId = _managedThreadIds[GetManagedThreadIdIndex(primitiveEvent)];

                            projectionExecutionContext.AddPrimitiveEvent(primitiveEvent, managedThreadId);
                        }
                    }

                    _projectionExecutionContexts.Add(projectionExecutionContext);
                }
                finally
                {
                    projectionExecutionContext.Lock.Release();
                }
            }
        }
        finally
        {
            _lock.Release();
        }

        if (_projectionExecutionContexts.Count == 0)
        {
            throw new ApplicationException(Resources.ProjectionConfigurationException);
        }
    }

    public async Task<ProjectionEvent?> RetrieveEventAsync(IPipelineContext<RetrieveEvent> pipelineContext, CancellationToken cancellationToken = default)
    {
        var processorThreadManagedThreadId = Guard.AgainstNull(pipelineContext).Pipeline.State.GetProcessorThreadManagedThreadId();

        if (_projectionExecutionContexts.Count == 0)
        {
            return null;
        }

        for (var i = 0; i < _projectionExecutionContexts.Count; i++)
        {
            await _lock.WaitAsync(cancellationToken);

            ProjectionExecutionContext projectionExecutionContext;

            try
            {
                var currentIndex = (_roundRobinIndex + i) % _projectionExecutionContexts.Count;
                projectionExecutionContext = _projectionExecutionContexts[currentIndex];

                if (projectionExecutionContext.BackoffTillDate > DateTimeOffset.UtcNow)
                {
                    continue;
                }
            }
            finally
            {
                _lock.Release();
            }

            await projectionExecutionContext.Lock.WaitAsync(cancellationToken);

            try
            {
                if (projectionExecutionContext.IsEmpty)
                {
                    await GetProjectionJournalAsync(projectionExecutionContext);
                }

                if (projectionExecutionContext.IsEmpty)
                {
                    projectionExecutionContext.Backoff();
                    continue;
                }

                projectionExecutionContext.Resume();

                var primitiveEvent = projectionExecutionContext.RetrievePrimitiveEvent(processorThreadManagedThreadId);

                if (primitiveEvent != null)
                {
                    return new(projectionExecutionContext.Projection, primitiveEvent);
                }
            }
            finally
            {
                projectionExecutionContext.Lock.Release();
            }
        }

        _roundRobinIndex = (_roundRobinIndex + 1) % _projectionExecutionContexts.Count;

        return null;
    }

    public async Task AcknowledgeEventAsync(IPipelineContext<AcknowledgeEvent> pipelineContext, CancellationToken cancellationToken = default)
    {
        var projectionEvent = Guard.AgainstNull(pipelineContext).Pipeline.State.GetProjectionEvent();

        await _lock.WaitAsync(cancellationToken);

        try
        {
            await _projectionRepository.CompleteAsync(projectionEvent, cancellationToken);

            var projectionExecutionContext = _projectionExecutionContexts.First(item => item.Projection.Name.Equals(projectionEvent.Projection.Name));

            await projectionExecutionContext.Lock.WaitAsync(cancellationToken);

            try
            {
                projectionExecutionContext.RemovePrimitiveEvent(projectionEvent.PrimitiveEvent);

                if (projectionExecutionContext.IsEmpty)
                {
                    await _projectionRepository.CommitJournalSequenceNumbersAsync(projectionEvent.Projection.Name, cancellationToken);

                    projectionExecutionContext.Commit();
                }
            }
            finally
            {
                projectionExecutionContext.Lock.Release();
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private int GetManagedThreadIdIndex(PrimitiveEvent primitiveEvent)
    {
        return ((primitiveEvent.CorrelationId ?? primitiveEvent.Id).GetHashCode() & int.MaxValue) % _managedThreadIds.Length;
    }

    private async Task GetProjectionJournalAsync(ProjectionExecutionContext projectionExecutionContext)
    {
        if (!projectionExecutionContext.IsEmpty || _managedThreadIds.Length == 0)
        {
            return;
        }

        var journalSequenceNumbers = new List<long>();

        using (new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled))
        {
            var specification = new PrimitiveEvent.Specification()
                .WithMaximumRows(_sqlServerEventProcessingOptions.ProjectionBatchSize)
                .WithSequenceNumberStart(projectionExecutionContext.Projection.SequenceNumber + 1);

            foreach (var primitiveEvent in (await _primitiveEventQuery.SearchAsync(specification)).OrderBy(item => item.SequenceNumber))
            {
                var managedThreadId = _managedThreadIds[GetManagedThreadIdIndex(primitiveEvent)];

                projectionExecutionContext.AddPrimitiveEvent(primitiveEvent, managedThreadId);

                journalSequenceNumbers.Add(primitiveEvent.SequenceNumber!.Value);
            }
        }

        await _projectionRepository.RegisterJournalSequenceNumbersAsync(projectionExecutionContext.Projection.Name, journalSequenceNumbers).ConfigureAwait(false);
    }

    private class ProjectionExecutionContext(Projection projection, IEnumerable<TimeSpan> backoffDurations)
    {
        private readonly TimeSpan[] _durations = Guard.AgainstEmpty(backoffDurations).ToArray();

        private readonly Dictionary<int, List<PrimitiveEvent>> _threadPrimitiveEvents = [];
        private int _durationIndex;
        public DateTimeOffset BackoffTillDate { get; private set; } = DateTimeOffset.MinValue;

        public bool IsEmpty => _threadPrimitiveEvents.Values.All(list => list.Count == 0);
        public SemaphoreSlim Lock { get; } = new(1, 1);

        public Projection Projection { get; } = projection;
        private long _commitSequenceNumber = 0;

        public void AddPrimitiveEvent(PrimitiveEvent primitiveEvent, int managedThreadId)
        {
            if (Lock.CurrentCount > 0)
            {
                throw new ApplicationException($"Lock required before invoking '{nameof(AddPrimitiveEvent)}'.");
            }

            if (!_threadPrimitiveEvents.TryGetValue(managedThreadId, out var primitiveEvents))
            {
                primitiveEvents = [];

                _threadPrimitiveEvents.Add(managedThreadId, primitiveEvents);
            }

            primitiveEvents.Add(Guard.AgainstNull(primitiveEvent));
        }

        public void Backoff()
        {
            if (_durationIndex >= _durations.Length)
            {
                _durationIndex = _durations.Length - 1;
            }

            BackoffTillDate = DateTimeOffset.UtcNow.Add(_durations[_durationIndex++]);
        }

        public void RemovePrimitiveEvent(PrimitiveEvent primitiveEvent)
        {
            if (Lock.CurrentCount > 0)
            {
                throw new ApplicationException($"Lock required before invoking '{nameof(RemovePrimitiveEvent)}'.");
            }

            var sequenceNumber = primitiveEvent.SequenceNumber!.Value;

            foreach (var list in _threadPrimitiveEvents.Values)
            {
                list.RemoveAll(e => e.SequenceNumber == sequenceNumber);
            }

            if (sequenceNumber > _commitSequenceNumber)
            {
                _commitSequenceNumber = sequenceNumber;
            }
        }

        public void Resume()
        {
            BackoffTillDate = DateTimeOffset.MinValue;
            _durationIndex = 0;
        }

        public PrimitiveEvent? RetrievePrimitiveEvent(int managedThreadId)
        {
            if (Lock.CurrentCount > 0)
            {
                throw new ApplicationException($"Lock required before invoking '{nameof(RetrievePrimitiveEvent)}'.");
            }

            if (!_threadPrimitiveEvents.TryGetValue(managedThreadId, out var primitiveEvents))
            {
                primitiveEvents = [];

                _threadPrimitiveEvents.Add(managedThreadId, primitiveEvents);
            }

            return primitiveEvents.OrderBy(item => item.SequenceNumber).FirstOrDefault();
        }

        public void Commit()
        {
            Projection.Commit(_commitSequenceNumber);
        }
    }
}