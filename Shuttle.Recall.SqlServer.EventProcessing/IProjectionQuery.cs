namespace Shuttle.Recall.SqlServer.EventProcessing;

public interface IProjectionQuery
{
    ValueTask<Projection?> GetAsync(CancellationToken cancellationToken = default);
    ValueTask<bool> HasPendingProjectionsAsync(long sequenceNumber, CancellationToken cancellationToken = default);
}