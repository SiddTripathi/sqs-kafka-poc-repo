using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Replay
{
    /// <summary>
    /// Serializable representation of a message suitable for storing in Blob.
    /// This will be written as a single JSON document per SQS message.
    /// </summary>
    public class ReplayRecord
    {
        // -------- Metadata --------
        public int Version { get; set; } = 1;
        public string Environment { get; set; } = "dev";
        public DateTime RecordedAtUtc { get; set; } = DateTime.UtcNow;
        public string? Source { get; set; }            // e.g. "sqs-to-kafka-worker"
        public string? PodName { get; set; }           // k8s pod name
        public string? NodeName { get; set; }          // k8s node

        // -------- SQS info --------
        public string? SqsMessageId { get; set; }
        public string? SqsReceiptHandle { get; set; }  // Mostly for debugging
        public IDictionary<string, string>? SqsAttributes { get; set; }
        public IDictionary<string, string>? MessageAttributes { get; set; }

        // -------- Payload --------
        /// <summary>
        /// Raw JSON body as received from SQS.
        /// </summary>
        public string? BodyJson { get; set; }

        /// <summary>
        /// Logical event type, usually from body["event"].
        /// </summary>
        public string? EventType { get; set; }

        public string? DedupId { get; set; }
        public string? KafkaKey { get; set; }

        // -------- Routing info --------
        public string? TargetTopic { get; set; }      // Topic chosen by routing
        public string? DefaultTopic { get; set; }
        public string? DlqTopic { get; set; }

        // -------- Result / outcome --------
        public ReplayStatus Status { get; set; } = ReplayStatus.Processed;
        public int? KafkaPartition { get; set; }
        public long? KafkaOffset { get; set; }
        public string? DlqReason { get; set; }
    }
}
