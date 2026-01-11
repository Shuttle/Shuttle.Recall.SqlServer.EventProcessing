using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public class SequentialProjectionEventService(IOptions<RecallOptions> recallOptions, ISequentialProjectionEventServiceContext sequentialProjectionEventServiceContext, SqlServerEventProcessingDbContext dbContext, IProjectionRepository projectionRepository, IProjectionQuery projectionQuery, IPrimitiveEventQuery primitiveEventQuery)
    : IProjectionEventService
{
    private readonly RecallOptions _recallOptions = Guard.AgainstNull(Guard.AgainstNull(recallOptions).Value);
    private readonly SqlServerEventProcessingDbContext _dbContext = Guard.AgainstNull(dbContext);
    private readonly IPrimitiveEventQuery _primitiveEventQuery = Guard.AgainstNull(primitiveEventQuery);
    private readonly IProjectionQuery _projectionQuery = Guard.AgainstNull(projectionQuery);
    private readonly IProjectionRepository _projectionRepository = Guard.AgainstNull(projectionRepository);
    private readonly ISequentialProjectionEventServiceContext _sequentialProjectionEventServiceContext = Guard.AgainstNull(sequentialProjectionEventServiceContext);
    private IDbContextTransaction? _transaction;

    public async Task AcknowledgeAsync(IPipelineContext<AcknowledgeEvent> pipelineContext, CancellationToken cancellationToken = default)
    {
        var projectionEvent = Guard.AgainstNull(pipelineContext).Pipeline.State.GetProjectionEvent();

        await _recallOptions.Operation.InvokeAsync(new($"[SequentialProjectionService.Acknowledge/Starting] : projection = '{projectionEvent.Projection.Name}' / sequence number = {projectionEvent.PrimitiveEvent.SequenceNumber}"), cancellationToken);

        await _projectionRepository.CommitAsync(projectionEvent.Projection, cancellationToken);

        if (_transaction != null)
        {
            await _transaction.CommitAsync(CancellationToken.None);
            await _transaction.DisposeAsync();
        }

        await _recallOptions.Operation.InvokeAsync(new($"[SequentialProjectionService.Acknowledge/Completed] : projection = '{projectionEvent.Projection.Name}' / sequence number = {projectionEvent.PrimitiveEvent.SequenceNumber}"), cancellationToken);
    }

    public async Task<ProjectionEvent?> RetrieveAsync(IPipelineContext<RetrieveEvent> pipelineContext, CancellationToken cancellationToken = default)
    {
        await _recallOptions.Operation.InvokeAsync(new($"[SequentialProjectionService.Retrieve/Starting]"), cancellationToken);
        
        if (System.Transactions.Transaction.Current == null)
        {
            _transaction = await _dbContext.Database.BeginTransactionAsync(cancellationToken);
        }

        var projection = await _projectionQuery.GetAsync(cancellationToken);

        if (projection == null)
        {
            await _recallOptions.Operation.InvokeAsync(new($"[SequentialProjectionService.Retrieve/Completed] : projection = <null>"), cancellationToken);
            return null;
        }

        var nextSequenceNumber = projection.SequenceNumber + 1;

        var primitiveEvent = await _sequentialProjectionEventServiceContext.RetrievePrimitiveEventAsync(_primitiveEventQuery, nextSequenceNumber, cancellationToken);

        await _recallOptions.Operation.InvokeAsync(new($"[SequentialProjectionService.Retrieve/Completed] : projection = '{projection.Name}' / sequence number = {primitiveEvent?.SequenceNumber.ToString() ?? "<null>"}"), cancellationToken);
        
        return primitiveEvent == null ? null : new(projection, primitiveEvent);
    }

    public async Task PipelineFailedAsync(IPipelineContext<PipelineFailed> pipelineContext, CancellationToken cancellationToken = new CancellationToken())
    {
        if (_transaction != null)
        {
            await _transaction.RollbackAsync(CancellationToken.None);
        }
    }
}