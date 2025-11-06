using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToKafka.Options
{
    public class SqsOptions
    {

        [Required]
        public string? Region { get; set; } = "ap-southeast-2";  //required

        [Required]
        public string QueueUrl { get; set; } = string.Empty;  //required
                                                              // Polling settings 


        //Optional
        public string? Profile { get; set; } = string.Empty;              // e.g., "default" (AWS profile)
        public int WaitTimeSeconds { get; set; } = 10;     // long polling
        public int MaxMessages { get; set; } = 5;          // batch size
        public int VisibilityTimeoutSeconds { get; set; } = 30;
        public int EmptyWaitDelayMs { get; set; } = 2000;  // backoff when queue is empty
    }
}
