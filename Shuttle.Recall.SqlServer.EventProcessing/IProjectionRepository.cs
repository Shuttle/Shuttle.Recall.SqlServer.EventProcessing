namespace Shuttle.Recall.SqlServer.EventProcessing;

public interface IProjectionRepository
{
    Task<Projection> GetAsync(string name, CancellationToken cancellationToken = default);
    Task CommitAsync(Projection projection, CancellationToken cancellationToken = default);
}