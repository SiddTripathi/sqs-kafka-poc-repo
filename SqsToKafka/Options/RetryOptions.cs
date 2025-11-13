using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Options
{
    public sealed class RetryOptions
    {
        [Range(1, 120000)]
        public int BaseDelayMs { get; set; } = 500;     // initial backoff

        [Range(1000, 300000)]
        public int MaxDelayMs { get; set; } = 30000;    // cap for backoff

        [Range(1, 50)]
        public int MaxProduceAttempts { get; set; } = 6; // total tries, including the first

        public bool Jitter { get; set; } = true;        // add randomness to spread load
    }
}
