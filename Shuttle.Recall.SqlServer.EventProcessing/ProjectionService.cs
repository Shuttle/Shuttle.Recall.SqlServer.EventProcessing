using System.Transactions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Threading;
using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public class ProjectionService(IOptions<SqlServerStorageOptions> sqlServerStorageOptions, IOptions<SqlServerEventProcessingOptions> sqlServerEventProcessingOptions, IDbContextFactory<SqlServerEventProcessingDbContext> dbContextFactory, IProjectionRepository projectionRepository, IProjectionQuery projectionQuery, IPrimitiveEventRepository primitiveEventRepository, IPrimitiveEventQuery primitiveEventQuery, IEventProcessorConfiguration eventProcessorConfiguration)
    : IProjectionService
{
    private readonly IDbContextFactory<SqlServerEventProcessingDbContext> _dbContextFactory = Guard.AgainstNull(dbContextFactory);
    private readonly SqlServerEventProcessingOptions _sqlServerEventProcessingOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerEventProcessingOptions).Value);
    private readonly IEventProcessorConfiguration _eventProcessorConfiguration = Guard.AgainstNull(eventProcessorConfiguration);
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly IPrimitiveEventQuery _primitiveEventQuery = Guard.AgainstNull(primitiveEventQuery);
    private readonly IPrimitiveEventRepository _primitiveEventRepository = Guard.AgainstNull(primitiveEventRepository);
    private readonly IProjectionQuery _projectionQuery = Guard.AgainstNull(projectionQuery);
    private readonly IProjectionRepository _projectionRepository = Guard.AgainstNull(projectionRepository);
    private readonly Dictionary<string, List<ThreadPrimitiveEvent>> _projectionThreadPrimitiveEvents = new();
    private readonly SqlServerStorageOptions _sqlServerStorageOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerStorageOptions).Value);
    private int[] _managedThreadIds = [];

    private Projection[] _projections = [];
    private int _roundRobinIndex;

    public async Task<ProjectionEvent?> RetrieveEventAsync(IPipelineContext<RetrieveEvent> pipelineContext, CancellationToken cancellationToken = default)
    {
        var processorThreadManagedThreadId = Guard.AgainstNull(pipelineContext).Pipeline.State.GetProcessorThreadManagedThreadId();

        Projection? projection;

        if (_projections.Length == 0)
        {
            return null;
        }

        await _lock.WaitAsync(cancellationToken);

        try
        {
            if (_roundRobinIndex >= _projections.Length)
            {
                _roundRobinIndex = 0;
            }

            projection = _projections[_roundRobinIndex++];
        }
        finally
        {
            _lock.Release();
        }

        var projectionThreadPrimitiveEvents = _projectionThreadPrimitiveEvents[projection.Name];

        if (!projectionThreadPrimitiveEvents.Any())
        {
            await GetProjectionJournalAsync(projection);
        }

        if (!projectionThreadPrimitiveEvents.Any())
        {
            return null;
        }

        var threadPrimitiveEvent = projectionThreadPrimitiveEvents.FirstOrDefault(item => item.ManagedThreadId == processorThreadManagedThreadId);

        return threadPrimitiveEvent == null ? null : new ProjectionEvent(projection, threadPrimitiveEvent.PrimitiveEvent);
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

            long sequenceNumberEnd;

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

            var journalSequenceNumberEnd = journalSequenceNumbers.Any() ? journalSequenceNumbers.Max() : 0;

            await _projectionRepository.RegisterJournalSequenceNumbersAsync(projection.Name, journalSequenceNumbers).ConfigureAwait(false);
            await _projectionRepository.SetSequenceNumberAsync(projection.Name, journalSequenceNumberEnd);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task StartupAsync(IProcessorThreadPool processorThreadPool)
    {
        Guard.AgainstNull(processorThreadPool);

        _managedThreadIds = processorThreadPool.ProcessorThreads.Select(item => item.ManagedThreadId).ToArray();

        Dictionary<string, List<long>> incompleteSequenceNumbers = new();

        List<Projection> projections = [];

        foreach (var projectionConfiguration in _eventProcessorConfiguration.Projections)
        {
            projections.Add(await _projectionRepository.GetAsync(projectionConfiguration.Name));

            _projectionThreadPrimitiveEvents.Add(projectionConfiguration.Name, []);

            incompleteSequenceNumbers.Add(projectionConfiguration.Name, [..await _projectionQuery.GetIncompleteSequenceNumbersAsync(projectionConfiguration.Name)]);
        }

        _projections = projections.ToArray();

        foreach (var pair in incompleteSequenceNumbers)
        {
            if (pair.Value.Count == 0)
            {
                continue;
            }

            var specification = new PrimitiveEvent.Specification();

            specification.WithSequenceNumbers(pair.Value);

            foreach (var primitiveEvent in (await _primitiveEventQuery.SearchAsync(specification)).OrderBy(item => item.SequenceNumber))
            {
                var managedThreadId = _managedThreadIds[GetManagedThreadIdIndex(primitiveEvent)];

                _projectionThreadPrimitiveEvents[pair.Key].Add(new(managedThreadId, primitiveEvent));
            }
        }
    }
}