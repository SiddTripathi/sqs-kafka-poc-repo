using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


using System.Threading;
using System.Threading.Tasks;

namespace SqsToKafka.Services;

public interface IKafkaProducer : IAsyncDisposable
{
    Task<bool> ProduceAsync(string topic, string? key, string value, CancellationToken ct);
}
