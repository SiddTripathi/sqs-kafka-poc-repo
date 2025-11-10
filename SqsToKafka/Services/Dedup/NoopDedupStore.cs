using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Services.Dedup
{
    /// <summary>
    /// Fallback when SQL dedup is disabled. Always says "not seen", does nothing.
    /// </summary>
    public sealed class NoopDedupStore : IDedupStore
    {
        public Task<bool> SeenBeforeAsync(string messageId, CancellationToken ct) => Task.FromResult(false);
        public Task MarkProcessedAsync(string messageId, DateTimeOffset ts, CancellationToken ct) => Task.CompletedTask;
        public Task<int> CleanupAsync(CancellationToken ct) => Task.FromResult(0);
    }
}
