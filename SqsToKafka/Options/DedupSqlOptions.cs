using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Options
{
    public sealed class DedupSqlOptions
    {
        public bool Enabled { get; set; } = false;   // turn SQL dedup on/off
        public string? ConnectionString { get; set; } // required when Enabled=true
        public int TtlDays { get; set; } = 7;         // how long to keep MessageIds
    }
}
