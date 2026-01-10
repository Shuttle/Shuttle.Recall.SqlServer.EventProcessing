using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public interface ISequentialProjectionEventServiceContext
{
    ValueTask<PrimitiveEvent?> RetrievePrimitiveEventAsync(IPrimitiveEventQuery primitiveEventQuery, long sequenceNumber, CancellationToken cancellationToken = default);
}