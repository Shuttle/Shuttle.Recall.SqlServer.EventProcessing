using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;
using Shuttle.Contract;
using Shuttle.Pipelines;
using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public class SequentialProjectionEventService(IOptions<RecallOptions> recallOptions, ISequentialProjectionEventServiceContext sequentialProjectionEventServiceContext, SqlServerStorageDbContext sqlServerStorageDbContext, SqlServerEventProcessingDbContext sqlServerEventProcessingDbContext, IProjectionRepository projectionRepository, IProjectionQuery projectionQuery, IProjectionEligibilityQuery projectionEligibilityQuery, IPrimitiveEventQuery primitiveEventQuery, IImmediateProjectionEventRepository immediateProjectionEventRepository)
    : IProjectionEventService
{
    private readonly RecallOptions _recallOptions = Guard.AgainstNull(Guard.AgainstNull(recallOptions).Value);
    private readonly SqlServerStorageDbContext _sqlServerStorageDbContext = Guard.AgainstNull(sqlServerStorageDbContext);
    private readonly SqlServerEventProcessingDbContext _sqlServerEventProcessingDbContext = Guard.AgainstNull(sqlServerEventProcessingDbContext);
    private readonly IImmediateProjectionEventRepository _immediateProjectionEventRepository = Guard.AgainstNull(immediateProjectionEventRepository);
    private readonly IPrimitiveEventQuery _primitiveEventQuery = Guard.AgainstNull(primitiveEventQuery);
    private readonly IProjectionQuery _projectionQuery = Guard.AgainstNull(projectionQuery);
    private readonly IProjectionEligibilityQuery _projectionEligibilityQuery = Guard.AgainstNull(projectionEligibilityQuery);
    private readonly IProjectionRepository _projectionRepository = Guard.AgainstNull(projectionRepository);
    private readonly ISequentialProjectionEventServiceContext _sequentialProjectionEventServiceContext = Guard.AgainstNull(sequentialProjectionEventServiceContext);
    private IDbContextTransaction? _transaction;

    public async Task AcknowledgeAsync(IPipelineContext<AcknowledgeEvent> pipelineContext, CancellationToken cancellationToken = default)
    {
        var projectionEvent = Guard.AgainstNull(pipelineContext).Pipeline.State.GetProjectionEvent();

        await _recallOptions.Operation.InvokeAsync(new($"[SequentialProjectionService.Acknowledge/Starting] : projection = '{projectionEvent.Projection.Name}' / sequence number = {projectionEvent.PrimitiveEvent.SequenceNumber}"), cancellationToken);

        await _projectionRepository.SaveAsync(projectionEvent.Projection, cancellationToken);

        if (projectionEvent.AlreadyHandled)
        {
            await _immediateProjectionEventRepository.RemoveAsync(projectionEvent.Projection.Name, projectionEvent.PrimitiveEvent.EventId, cancellationToken);
        }

        if (_transaction != null)
        {
            await _transaction.CommitAsync(CancellationToken.None);
            await _transaction.DisposeAsync();
        }

        await _recallOptions.Operation.InvokeAsync(new($"[SequentialProjectionService.Acknowledge/Completed] : projection = '{projectionEvent.Projection.Name}' / sequence number = {projectionEvent.PrimitiveEvent.SequenceNumber}"), cancellationToken);
    }

    public async Task<ProjectionEvent?> RetrieveAsync(IPipelineContext<RetrieveEvent> pipelineContext, CancellationToken cancellationToken = default)
    {
        await _recallOptions.Operation.InvokeAsync(new("[SequentialProjectionService.Retrieve/Starting]"), cancellationToken);

        // Cheap, non-transactional pre-check: on an idle pass (the common case) this avoids opening a transaction
        // and running the locking claim query in `GetPendingAsync` on every single poll.
        if (!await _projectionEligibilityQuery.HasEligibleProjectionAsync(cancellationToken))
        {
            await _recallOptions.Operation.InvokeAsync(new("[SequentialProjectionService.Retrieve/Completed] : projection = <null>"), cancellationToken);
            return null;
        }

        _transaction = await _sqlServerEventProcessingDbContext.Database.BeginTransactionAsync(cancellationToken);
        await _sqlServerStorageDbContext.Database.UseTransactionAsync(_transaction.GetDbTransaction(), cancellationToken);

        var projection = await _projectionQuery.GetPendingAsync(cancellationToken);

        if (projection == null)
        {
            // Lost the race for the eligible projection found above (another thread/process claimed it between
            // the pre-check and this attempt) — nothing was written, but close the transaction deterministically
            // rather than leaving it for the DbContext's disposal to abandon implicitly.
            await _recallOptions.Operation.InvokeAsync(new("[SequentialProjectionService.Retrieve/Completed] : projection = <null>"), cancellationToken);
            await RollbackTransactionAsync(cancellationToken);
            return null;
        }

        var nextSequenceNumber = projection.SequenceNumber + 1;

        var primitiveEvent = await _sequentialProjectionEventServiceContext.RetrievePrimitiveEventAsync(_primitiveEventQuery, nextSequenceNumber, cancellationToken);

        await _recallOptions.Operation.InvokeAsync(new($"[SequentialProjectionService.Retrieve/Completed] : projection = '{projection.Name}' / sequence number = {primitiveEvent?.SequenceNumber.ToString() ?? "<null>"}"), cancellationToken);

        if (primitiveEvent == null)
        {
            // The projection was claimed (its `LockedAt` was set) but there is no next event for it yet — roll
            // that claim back immediately instead of leaving it to the DbContext's disposal, so other threads
            // stop skipping past this row (via `READPAST`) sooner rather than waiting out `ProjectionLockTimeout`.
            await RollbackTransactionAsync(cancellationToken);
            return null;
        }

        var alreadyHandled = await _immediateProjectionEventRepository.ContainsAsync(projection.Name, primitiveEvent.EventId, cancellationToken);

        return new(new(projection.Name, projection.SequenceNumber, projection.FailureCount), primitiveEvent, alreadyHandled);
    }

    private async Task RollbackTransactionAsync(CancellationToken cancellationToken)
    {
        if (_transaction == null)
        {
            return;
        }

        await _transaction.RollbackAsync(cancellationToken);
        await _transaction.DisposeAsync();

        _transaction = null;
    }

    public async Task ProjectionEventHandledAsync(string projectionName, Guid eventId, CancellationToken cancellationToken = default)
    {
        await _immediateProjectionEventRepository.SaveAsync(projectionName, eventId, cancellationToken);
    }

    public async Task DeferAsync(IPipelineContext<HandleEvent> pipelineContext, CancellationToken cancellationToken = default)
    {
        var projectionEvent = Guard.AgainstNull(pipelineContext).Pipeline.State.GetProjectionEvent();
        var deferredUntil = Guard.AgainstNull(pipelineContext).Pipeline.State.GetDeferredUntil();

        if (!deferredUntil.HasValue)
        {
            return;
        }

        await _projectionRepository.SaveAsync(projectionEvent.Projection.Defer(deferredUntil.Value), cancellationToken);

        if (_transaction != null)
        {
            await _transaction.CommitAsync(CancellationToken.None);
            await _transaction.DisposeAsync();
        }
    }

    public async Task PipelineFailedAsync(IPipelineContext<PipelineFailed> pipelineContext, CancellationToken cancellationToken = default)
    {
        await RollbackTransactionAsync(CancellationToken.None);

        var projectionEvent = Guard.AgainstNull(pipelineContext).Pipeline.State.TryGetProjectionEvent();

        if (projectionEvent == null)
        {
            return;
        }

        projectionEvent.Projection.Failed();

        var delay = GetFailureDuration(_recallOptions.EventProcessing.ProjectionProcessorFailureDurations, projectionEvent.Projection.FailureCount);

        await _projectionRepository.SaveAsync(projectionEvent.Projection.Defer(DateTimeOffset.UtcNow.Add(delay)), CancellationToken.None);
    }

    private static TimeSpan GetFailureDuration(IReadOnlyList<TimeSpan> durations, int failureCount)
    {
        if (durations.Count == 0)
        {
            return TimeSpan.FromSeconds(15);
        }

        var index = Math.Min(failureCount - 1, durations.Count - 1);

        return durations[Math.Max(index, 0)];
    }
}