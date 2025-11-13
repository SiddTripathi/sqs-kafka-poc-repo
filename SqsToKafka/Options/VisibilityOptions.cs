using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Options
{
    public sealed class VisibilityOptions
    {
        // Turn auto-renew on/off
        public bool AutoExtend { get; set; } = true;

        // How often to extend (seconds). Choose < Sqs.VisibilityTimeoutSeconds.
        public int StepSeconds { get; set; } = 25;

        // Safety cap for total time we’ll keep extending a single message (seconds)
        public int MaxPerMessageSeconds { get; set; } = 300;
    }
}
