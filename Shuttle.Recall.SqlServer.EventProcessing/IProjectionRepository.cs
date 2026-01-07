namespace Shuttle.Recall.SqlServer.EventProcessing;

public interface IProjectionRepository
{
    Task<Projection> GetAsync(string name, CancellationToken cancellationToken = default);
    Task CommitJournalSequenceNumbersAsync(string name, CancellationToken cancellationToken = default);
    Task RegisterJournalSequenceNumbersAsync(string name, IEnumerable<long> sequenceNumbers, CancellationToken cancellationToken = default);
    Task CompleteAsync(ProjectionEvent projectionEvent, CancellationToken cancellationToken = default);
}