using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqsToKafka.Options;

namespace SqsToKafka.Services.Dedup
{

    public sealed class InMemoryDedupCache : IDedupCache, IDisposable
    {
        private readonly ILogger<InMemoryDedupCache> _logger;
        private readonly DedupOptions _opts;
        private readonly ConcurrentDictionary<string, DateTimeOffset> _map = new();
        private readonly Timer _sweeper;

        public InMemoryDedupCache(IOptions<DedupOptions> options, ILogger<InMemoryDedupCache> logger)
        {
            _logger = logger;
            _opts = options.Value;
            _sweeper = new Timer(Sweep, null, TimeSpan.FromSeconds(_opts.CleanupIntervalSeconds),
                TimeSpan.FromSeconds(_opts.CleanupIntervalSeconds));
        }

        public bool TryAcquire(string id, TimeSpan ttl)
        {
            // quick safety cap
            if (_map.Count >= _opts.MaxEntries)
            {
                _logger.LogWarning("Dedup store at capacity {Count}/{Cap}. Rejecting id {Id}.",
                    _map.Count, _opts.MaxEntries, id);
                return false;
            }

            var expiresAt = DateTimeOffset.UtcNow + ttl;
            var added = _map.TryAdd(id, expiresAt);
            return added; // false => already present (duplicate)
        }

        public void Release(string id)
        {
            _map.TryRemove(id, out _);
        }

        private void Sweep(object? _)
        {
            var now = DateTimeOffset.UtcNow;
            var removed = 0;
            foreach (var kv in _map)
            {
                if (kv.Value <= now && _map.TryRemove(kv.Key, out DateTimeOffset _))
                    removed++;
            }
            if (removed > 0)
                _logger.LogDebug("Dedup sweep removed {Count} expired entries. Remaining: {Remain}.",
                    removed, _map.Count);
        }

        public void Dispose() => _sweeper.Dispose();
    }
}
