using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Options
{
    public class KafkaOptions
    {
        public string BootstrapServers { get; set; } = string.Empty;
        // Producer tuning (optional for later)
        public string Acks { get; set; } = "all";
        public int? MessageTimeoutMs { get; set; } = 30000;
        // Optional: auth/SSL (we’ll fill later if your on-prem Kafka needs it)
        public string? SaslUsername { get; set; }
        public string? SaslPassword { get; set; }
        public string? SecurityProtocol { get; set; } // e.g., "SaslSsl"
        public string? SaslMechanism { get; set; }    // e.g., "PLAIN"
        public string DefaultTopic { get; set; } = "sqs.to.kafka.dev";

    }
}
