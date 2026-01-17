using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Recall.SqlServer.Storage;
using System.Collections.Concurrent;
using System.Transactions;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public class SequentialProjectionEventServiceContext(IOptions<RecallOptions> recallOptions, IOptions<SqlServerEventProcessingOptions> sqlServerEventProcessingOptions) : ISequentialProjectionEventServiceContext
{
    private readonly RecallOptions _recallOptions = Guard.AgainstNull(Guard.AgainstNull(recallOptions).Value);
    private readonly SqlServerEventProcessingOptions _sqlServerEventProcessingOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerEventProcessingOptions).Value);
    private readonly PrimitiveEventCache _cache = new(maximumSize: sqlServerEventProcessingOptions.Value.MaximumCacheSize, cacheDuration: sqlServerEventProcessingOptions.Value.CacheDuration);

    public async ValueTask<PrimitiveEvent?> RetrievePrimitiveEventAsync(IPrimitiveEventQuery primitiveEventQuery, long sequenceNumber, CancellationToken cancellationToken = default)
    {
        await _recallOptions.Operation.InvokeAsync(new($"[SequentialProjectionEventServiceContext.Retrieve/Starting] : sequence number = {sequenceNumber}"), cancellationToken);

        if (!_cache.TryGet(sequenceNumber, out var cachedPrimitiveEvent))
        {
            await _recallOptions.Operation.InvokeAsync(new($"[SequentialProjectionEventServiceContext.Retrieve/Cache:Miss] : sequence number = {sequenceNumber}"), cancellationToken);

            using (new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled))
            {
                var specification = new PrimitiveEvent.Specification()
                    .WithMaximumRows(_sqlServerEventProcessingOptions.ProjectionPrefetchCount)
                    .WithSequenceNumberStart(sequenceNumber);

                var primitiveEvents = (await primitiveEventQuery.SearchAsync(specification, cancellationToken))
                    .OrderBy(item => item.SequenceNumber)
                    .ToList();

                foreach (var primitiveEvent in primitiveEvents)
                {
                    _cache.Add(primitiveEvent.SequenceNumber!.Value, primitiveEvent);
                }

                await _recallOptions.Operation.InvokeAsync(new($"[SequentialProjectionEventServiceContext.Retrieve/Search] : sequence number = {sequenceNumber} / primitive event count = {primitiveEvents.Count} / cache size = {_cache.Count}"), cancellationToken);

                // This would handle gaps (SequenceNumber >= sequenceNumber) although that really should not happen.
                // Adding for legacy, but clearing projections and re-sequencing would be the preferred approach.
                cachedPrimitiveEvent = primitiveEvents.FirstOrDefault(e => e.SequenceNumber >= sequenceNumber);
            }
        }

        await _recallOptions.Operation.InvokeAsync(new($"[SequentialProjectionEventServiceContext.Retrieve/Search] : sequence number = {sequenceNumber} / primitive event sequence number = {cachedPrimitiveEvent?.SequenceNumber.ToString() ?? "<null>"} / cache size = {_cache.Count}"), cancellationToken);

        return cachedPrimitiveEvent;
    }

    private class PrimitiveEventCache(int maximumSize, TimeSpan cacheDuration)
    {
        private readonly ConcurrentDictionary<long, CacheEntry> _cache = new();
        private readonly Queue<long> _insertionOrder = new();
        private readonly Lock _evictionLock = new();

        public int Count => _cache.Count;

        public bool TryGet(long key, out PrimitiveEvent? value)
        {
            if (_cache.TryGetValue(key, out var entry))
            {
                if (DateTime.UtcNow - entry.Timestamp <= cacheDuration)
                {
                    value = entry.PrimitiveEvent;
                    return true;
                }

                _cache.TryRemove(key, out _);
            }

            value = null;
            return false;
        }

        public void Add(long key, PrimitiveEvent primitiveEvent)
        {
            var entry = new CacheEntry(primitiveEvent, DateTime.UtcNow);

            if (!_cache.TryAdd(key, entry))
            {
                return;
            }

            lock (_evictionLock)
            {
                _insertionOrder.Enqueue(key);
                Eviction();
            }
        }

        private void Eviction()
        {
            while (_insertionOrder.Count > maximumSize)
            {
                if (_insertionOrder.TryDequeue(out var oldestSequenceNumber))
                {
                    _cache.TryRemove(oldestSequenceNumber, out _);
                }
            }

            var now = DateTime.UtcNow;

            while (_insertionOrder.Count > 0)
            {
                if (_insertionOrder.TryPeek(out var oldestSequenceNumber) && _cache.TryGetValue(oldestSequenceNumber, out var entry) && now - entry.Timestamp > cacheDuration)
                {
                    _insertionOrder.Dequeue();
                    _cache.TryRemove(oldestSequenceNumber, out _);
                }
                else
                {
                    break;
                }
            }
        }

        private record CacheEntry(PrimitiveEvent PrimitiveEvent, DateTime Timestamp);
    }
}