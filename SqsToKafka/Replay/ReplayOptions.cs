using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Replay
{
    /// <summary>
    /// Configuration for the replay / blob-recording feature.
    /// Bound from configuration section "Replay".
    /// </summary>
    public class ReplayOptions
    {
        /// <summary>
        /// Master switch to enable/disable recording to Blob.
        /// </summary>
        public bool Enabled { get; set; } = false;

        /// <summary>
        /// Connection string or SAS-based connection string for the storage account.
        /// This will typically come from Kubernetes Secret, not from appsettings.json.
        /// </summary>
        public string? ConnectionString { get; set; }

        /// <summary>
        /// Name of the blob container, e.g. "sqskafka-replay-dev".
        /// </summary>
        public string? ContainerName { get; set; }

        /// <summary>
        /// Base virtual folder under the container, e.g. "by-time".
        /// </summary>
        public string BasePath { get; set; } = "by-time";

        /// <summary>
        /// Logical environment tag to include in metadata, e.g. "dev", "fst", "prod".
        /// </summary>
        public string EnvironmentTag { get; set; } = "dev";
    }
}
