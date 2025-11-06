using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Collections.Concurrent;

namespace SqsToKafka.Services;

public interface IDedupCache
{
    bool IsDuplicate(string messageId);
    void MarkProcessed(string messageId);
}

public class InMemoryDedupCache : IDedupCache
{
    private readonly ConcurrentDictionary<string, DateTime> _cache = new();
    private readonly TimeSpan _ttl = TimeSpan.FromMinutes(10);
    private readonly object _cleanupLock = new();

    public bool IsDuplicate(string messageId)
    {
        Cleanup();
        return _cache.ContainsKey(messageId);
    }

    public void MarkProcessed(string messageId)
    {
        _cache[messageId] = DateTime.UtcNow;
    }

    private void Cleanup()
    {
        var now = DateTime.UtcNow;
        lock (_cleanupLock)
        {
            foreach (var key in _cache.Keys)
            {
                if (now - _cache[key] > _ttl)
                    _cache.TryRemove(key, out _);
            }
        }
    }
}

