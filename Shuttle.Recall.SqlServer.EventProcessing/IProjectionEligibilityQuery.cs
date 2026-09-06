namespace Shuttle.Recall.SqlServer.EventProcessing;

public interface IProjectionEligibilityQuery
{
    /// <summary>
    /// Cheap, non-transactional existence check for whether any projection currently qualifies to be claimed by
    /// <see cref="IProjectionQuery.GetPendingAsync"/> (i.e. not locked, and not deferred into the future). Intended
    /// as a poll-loop pre-check so that callers avoid opening a transaction and running the locking claim query on
    /// every idle pass. The result is advisory only — a caller must still use
    /// <see cref="IProjectionQuery.GetPendingAsync"/> to actually claim a projection, since eligibility can change
    /// between this check and the claim attempt.
    /// </summary>
    ValueTask<bool> HasEligibleProjectionAsync(CancellationToken cancellationToken = default);
}
