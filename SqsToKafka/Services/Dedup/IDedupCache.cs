using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Services.Dedup
{

    /// <summary>
    /// A very small "lease" style dedup store:
    /// - TryAcquire returns false if the id is already present (duplicate)
    /// - Release removes the id (used when processing fails)
    /// Entries expire automatically after TTL.
    public interface IDedupCache
    {
        bool TryAcquire(string id, TimeSpan ttl);
        void Release(string id);
    }
}
