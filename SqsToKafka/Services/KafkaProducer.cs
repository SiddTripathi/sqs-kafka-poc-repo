using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqsToKafka.Options;

namespace SqsToKafka.Services;

public sealed class KafkaProducer : IKafkaProducer
{
    private readonly ILogger<KafkaProducer> _logger;
    private readonly IProducer<string, string> _producer;
    private readonly KafkaOptions _opts;

    public KafkaProducer(IOptions<KafkaOptions> options, ILogger<KafkaProducer> logger)
    {
        _logger = logger;
        _opts = options.Value;

        var cfg = new ProducerConfig
        {
            BootstrapServers = _opts.BootstrapServers,
            Acks = Acks.All,                 // wait for all replicas
            EnableIdempotence = true,        // strong delivery guarantees. prevents duplicate writes from the same producer session.
            MessageTimeoutMs = _opts.MessageTimeoutMs ?? 30000
        };

        // Optional SASL/SSL (fill later if your on-prem Kafka needs it)
        if (!string.IsNullOrWhiteSpace(_opts.SecurityProtocol))
            cfg.SecurityProtocol = Enum.Parse<SecurityProtocol>(_opts.SecurityProtocol, ignoreCase: true);
        if (!string.IsNullOrWhiteSpace(_opts.SaslMechanism))
            cfg.SaslMechanism = Enum.Parse<SaslMechanism>(_opts.SaslMechanism, ignoreCase: true);
        if (!string.IsNullOrWhiteSpace(_opts.SaslUsername))
            cfg.SaslUsername = _opts.SaslUsername;
        if (!string.IsNullOrWhiteSpace(_opts.SaslPassword))
            cfg.SaslPassword = _opts.SaslPassword;

        _producer = new ProducerBuilder<string, string>(cfg)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka error: {Reason}", e.Reason))
            .Build();
    }

    public async Task<bool> ProduceAsync(string topic, string? key, string value, CancellationToken ct)
    {
        try
        {
            var dr = await _producer.ProduceAsync(
                topic,
                new Message<string, string> { Key = key ?? string.Empty, Value = value },
                ct);

            var persisted = dr.Status == PersistenceStatus.Persisted;
            if (persisted)
                _logger.LogInformation("Kafka persisted to {Topic} @ {PartitionOffset}", dr.Topic, dr.TopicPartitionOffset);
            else
                _logger.LogWarning("Kafka delivery not persisted. Status: {Status}", dr.Status);

            return persisted;
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex, "Kafka produce failed for topic {Topic}", topic);
            return false;
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Flush outstanding messages
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        await Task.CompletedTask;
    }
}

