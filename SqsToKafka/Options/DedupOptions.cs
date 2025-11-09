using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Options
{
    public sealed class DedupOptions
    {
        public bool Enabled { get; set; } = true;

        /// <summary>Entry time-to-live (seconds).</summary>
        public int TtlSeconds { get; set; } = 600; // 10 min default

        /// <summary>Cleanup sweep interval (seconds).</summary>
        public int CleanupIntervalSeconds { get; set; } = 30;

        /// <summary>Safety cap to avoid unbounded growth.</summary>
        public int MaxEntries { get; set; } = 50_000;
    }
}
