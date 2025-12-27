namespace Shuttle.Recall.SqlServer.EventProcessing;

public interface IProjectionQuery
{
    Task<IEnumerable<long>> GetIncompleteSequenceNumbersAsync(string name, CancellationToken cancellationToken = default);
}