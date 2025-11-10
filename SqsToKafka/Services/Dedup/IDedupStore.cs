using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Threading.Tasks;
namespace SqsToKafka.Services.Dedup
{
    public interface IDedupStore
    {
        /// <summary>Returns true if this MessageId was processed before.</summary>
        Task<bool> SeenBeforeAsync(string messageId, CancellationToken ct);

        /// <summary>Marks a MessageId as processed.</summary>
        Task MarkProcessedAsync(string messageId, DateTimeOffset processedAt, CancellationToken ct);

        /// <summary>Deletes entries older than TTL. Return number of rows deleted.</summary>
        Task<int> CleanupAsync(CancellationToken ct);
    }
}
