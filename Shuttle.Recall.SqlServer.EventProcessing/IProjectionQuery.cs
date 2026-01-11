namespace Shuttle.Recall.SqlServer.EventProcessing;

public interface IProjectionQuery
{
    ValueTask<Projection?> GetAsync(CancellationToken cancellationToken = default);
}