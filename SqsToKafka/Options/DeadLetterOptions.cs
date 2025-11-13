using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Options
{
    public sealed class DeadLetterOptions
    {
        public bool Enabled { get; set; } = false;   // turn DLQ publishing on/off
        public string Topic { get; set; } = "routed.dead";
        public bool IncludeBody { get; set; } = true;   // payload in DLQ event
        public bool IncludeAttributes { get; set; } = true; // SQS attributes snapshot
    }
}
