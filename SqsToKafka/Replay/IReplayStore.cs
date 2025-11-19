using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Replay
{
    /// <summary>
    /// Abstraction for persisting replay records (e.g. to Azure Blob Storage).
    /// </summary>
    public interface IReplayStore
    {
        /// <summary>
        /// Persist a single replay record.
        /// Implementations should be idempotent for the same blob name if possible.
        /// </summary>
        Task RecordAsync(ReplayRecord record, CancellationToken cancellationToken);
    }
}
