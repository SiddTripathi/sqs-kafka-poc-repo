using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Replay
{
    /// <summary>
    /// High-level outcome of processing for replay/audit purposes.
    /// </summary>
    public enum ReplayStatus
    {
        Processed = 0,     // Normal success path → Kafka
        DeadLettered = 1,  // Went to Kafka DLQ
        Failed = 2         // Permanently failed without DLQ (should be rare)
    }
}
