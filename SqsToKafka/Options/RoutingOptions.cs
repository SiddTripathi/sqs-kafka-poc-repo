using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Options
{
    public class RoutingOptions
    {
        // Which JSON field to read from the message body (if present)
        public string BodyField { get; set; } = "event";

        // Map of <fieldValue> -> <topic>
        public Dictionary<string, string> Map { get; set; } = new();
    }
}
