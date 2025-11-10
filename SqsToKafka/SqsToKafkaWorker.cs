using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqsToKafka.Options;
using SqsToKafka.Services;
using SqsToKafka.Services.Dedup;
using System.Text.Json;




namespace SqsToKafka;

public class SqsToKafkaWorker : BackgroundService
{
    private readonly ILogger<SqsToKafkaWorker> _logger;
    private readonly KafkaOptions _kafka;
    private readonly SqsOptions _sqs;
    private readonly IKafkaProducer _producer;
    private readonly RoutingOptions _routing;
    private readonly IDedupCache _dedup;
    private readonly DedupOptions _dedupOpts;
    private readonly IDedupStore _dedupStore;

    public SqsToKafkaWorker(
        ILogger<SqsToKafkaWorker> logger,
        IOptions<KafkaOptions> kafka,
        IOptions<SqsOptions> sqs,
        IKafkaProducer producer,
        IOptions<RoutingOptions> routing,
        IDedupCache dedup,
        IOptions<DedupOptions> dedupOptions,
        IDedupStore dedupStore)
    {
        _logger = logger;
        _kafka = kafka.Value;
        _sqs = sqs.Value;
        _producer = producer;
        _routing = routing.Value;
        _dedup = dedup;
        _dedupOpts = dedupOptions.Value;
        _dedupStore = dedupStore;

        _logger.LogInformation(
         string.IsNullOrWhiteSpace(_sqs.Profile)
             ? "No AWS profile set — will use environment credentials."
             : $"Using AWS profile: {_sqs.Profile}"
         );


    }

    private (string topic, string? key) ResolveRouting(Amazon.SQS.Model.Message msg)
    {
        // 1) Start with defaults
        string? topic = null;
        string? key = null;

        // 2) SQS message attributes take precedence if present
        if (msg.MessageAttributes != null)
        {
            if (msg.MessageAttributes.TryGetValue("KafkaTopic", out var ta))
                topic = ta.StringValue;
            if (msg.MessageAttributes.TryGetValue("KafkaKey", out var ka))
                key = ka.StringValue;
        }

        // 3) If still missing, try JSON body (best-effort)
        if (string.IsNullOrWhiteSpace(topic))
        {
            try
            {
                using var doc = JsonDocument.Parse(msg.Body);
                var root = doc.RootElement;

                if (root.ValueKind == JsonValueKind.Object)
                {
                    // topic could be "topic" or "kafkaTopic"
                    if (root.TryGetProperty("kafkaTopic", out var t1) && t1.ValueKind == JsonValueKind.String)
                        topic = t1.GetString();
                    else if (root.TryGetProperty("topic", out var t2) && t2.ValueKind == JsonValueKind.String)
                        topic = t2.GetString();

                    // Or route by a specific field (default: "event") via Map
                    if (string.IsNullOrWhiteSpace(topic) && !string.IsNullOrWhiteSpace(_routing.BodyField))
                    {
                        if (root.TryGetProperty(_routing.BodyField, out var field) && field.ValueKind == JsonValueKind.String)
                        {
                            var fieldValue = field.GetString();
                            if (fieldValue != null && _routing.Map.TryGetValue(fieldValue, out var mappedTopic))
                                topic = mappedTopic;
                        }
                    }


                    // key could be "key" or "kafkaKey"
                    if (string.IsNullOrWhiteSpace(key))
                    {
                        if (root.TryGetProperty("kafkaKey", out var k1) && k1.ValueKind == JsonValueKind.String)
                            key = k1.GetString();
                        else if (root.TryGetProperty("key", out var k2) && k2.ValueKind == JsonValueKind.String)
                            key = k2.GetString();
                    }
                }
            }
            catch
            {
                // Non-JSON body or malformed JSON: ignore and fall back to defaults
            }
        }

        return (string.IsNullOrWhiteSpace(topic) ? _kafka.DefaultTopic : topic, key);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting SQS polling for queue: {QueueUrl}", _sqs.QueueUrl);

        var region = RegionEndpoint.GetBySystemName(_sqs.Region);
        AWSCredentials creds;
        try
        {

            if (!string.IsNullOrWhiteSpace(_sqs.Profile))
            {

                var sharedFile = new SharedCredentialsFile(); // reads ~/.aws/credentials
                if (sharedFile.TryGetProfile(_sqs.Profile, out var profile) &&
                    AWSCredentialsFactory.TryGetAWSCredentials(profile, sharedFile, out creds))
                {
                    _logger.LogInformation("Loaded AWS profile '{Profile}' successfully.", _sqs.Profile);
                }
                else
                {
                    throw new Exception($"AWS profile '{_sqs.Profile}' not found in credentials file.");
                }
            }
            else
            {
                // Fallback: environment variables, container role, etc.
                creds = Amazon.Runtime.FallbackCredentialsFactory.GetCredentials();
                _logger.LogInformation("Using default AWS credential chain (env/metadata).");
            }
        
        
        
        
        
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load AWS credentials profile '{Profile}'", _sqs.Profile);
            throw;
        }

        var sqsClient = new AmazonSQSClient(creds, region);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var request = new ReceiveMessageRequest
                    {
                        QueueUrl = _sqs.QueueUrl,
                        MaxNumberOfMessages = _sqs.MaxMessages,
                        WaitTimeSeconds = _sqs.WaitTimeSeconds,
                        VisibilityTimeout = _sqs.VisibilityTimeoutSeconds,
                        MessageAttributeNames = new List<string> { "All" },
                        MessageSystemAttributeNames = new List<string> { "ApproximateReceiveCount" }
                    };

                    var response = await sqsClient.ReceiveMessageAsync(request, stoppingToken);

                    if (response.Messages != null && response.Messages.Count > 0)
                    {
                        _logger.LogInformation("Received {Count} messages.", response.Messages.Count);

                        foreach (var msg in response.Messages)
                        {
                            _logger.LogInformation("Message ID: {Id} | Body: {Body}", msg.MessageId, msg.Body);

                            /* 
                             * if (_dedup.TryAcquire(msg.MessageId, TimeSpan.FromSeconds(_dedupOpts.TtlSeconds)))
                             //{
                                 _logger.LogWarning("[Dedup] Duplicate message skipped: {Id}", msg.MessageId);
                                 continue;
                             //}


                             MessageAttributeValue topicAttr = null!;
                             MessageAttributeValue keyAttr = null!;

                             if (msg.MessageAttributes != null)
                             {
                                 msg.MessageAttributes.TryGetValue("KafkaTopic", out topicAttr);
                                 msg.MessageAttributes.TryGetValue("KafkaKey", out keyAttr);
                             }

                             var topic1 = string.IsNullOrWhiteSpace(topicAttr?.StringValue)
                                 ? _kafka.DefaultTopic
                                 : topicAttr.StringValue;

                             var key1 = keyAttr?.StringValue;

                             var (topic, key) = ResolveRouting(msg);

                             */

                            if (await _dedupStore.SeenBeforeAsync(msg.MessageId, stoppingToken))
                            {
                                _logger.LogWarning("[Dedup:SQL] Already processed MessageId={MessageId} — skipping.", msg.MessageId);
                                // Do NOT delete from SQS here; let redrive/visibility policies handle it if needed.
                                // If you WANT to delete already-seen messages, uncomment:
                                // await sqsClient.DeleteMessageAsync(_sqs.QueueUrl, msg.ReceiptHandle, stoppingToken);
                                continue;
                            }


                            if (_dedupOpts.Enabled)
                            {
                                var ttl = TimeSpan.FromSeconds(_dedupOpts.TtlSeconds);
                                if (!_dedup.TryAcquire(msg.MessageId, ttl))
                                {
                                    _logger.LogWarning("[Dedup] Duplicate message skipped: {MessageId}", msg.MessageId);
                                    //await sqsClient.DeleteMessageAsync(_sqs.QueueUrl, msg.ReceiptHandle, stoppingToken);
                                    continue;
                                }
                            }

                            var produced = false;
                            try
                            {
                                var (topic, key) = ResolveRouting(msg);
                                // 1) Produce to Kafka
                                produced = await _producer.ProduceAsync(topic, key, msg.Body, stoppingToken);

                                if (produced)
                                {
                                    //_dedup.Release(msg.MessageId);
                                    await sqsClient.DeleteMessageAsync(_sqs.QueueUrl, msg.ReceiptHandle, stoppingToken);
                                    _logger.LogInformation("Deleted message {Id} from SQS after Kafka ack.", msg.MessageId);
                                    await _dedupStore.MarkProcessedAsync(msg.MessageId, DateTimeOffset.UtcNow, stoppingToken);
                                }
                                else
                                {
                                    _logger.LogWarning("Kafka failed for message {Id}. Not deleting from SQS.", msg.MessageId);

                                    // Allow retry: free the in-flight lock
                                    if (_dedupOpts.Enabled) _dedup.Release(msg.MessageId);

                                    // Optional back-off
                                    var extendBySeconds = Math.Min(60, _sqs.VisibilityTimeoutSeconds);
                                    try
                                    {
                                        await sqsClient.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                                        {
                                            QueueUrl = _sqs.QueueUrl,
                                            ReceiptHandle = msg.ReceiptHandle,
                                            VisibilityTimeout = extendBySeconds
                                        }, stoppingToken);

                                        var rc = (msg.Attributes != null &&
                                                  msg.Attributes.TryGetValue("ApproximateReceiveCount", out var rcStr) &&
                                                  int.TryParse(rcStr, out var r)) ? r : 1;

                                        _logger.LogWarning(
                                            "ReceiveCount={ReceiveCount}. Extended visibility by {Seconds}s for {Id}.",
                                            rc, extendBySeconds, msg.MessageId);
                                    }
                                    catch (Exception ex2)
                                    {
                                        _logger.LogError(ex2, "Failed to extend visibility for {Id}.", msg.MessageId);
                                    }



                                }
                            } 
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error producing message {Id}.", msg.MessageId);
                                if (_dedupOpts.Enabled) _dedup.Release(msg.MessageId);  // free lock on unexpected error
                            }
                            // 2) Delete from SQS only on success

                            /*else
                            {
                                if (_dedupOpts.Enabled)
                                    _dedup.Release(msg.MessageId);

                                var receiveCount = msg.Attributes != null &&
                                                   msg.Attributes.TryGetValue("ApproximateReceiveCount", out var rcStr) &&
                                                   int.TryParse(rcStr, out var rc) ? rc : 1;

                                // Extend visibility so it won’t reappear immediately while Kafka is recovering.
                                var extendBySeconds = Math.Min(60, _sqs.VisibilityTimeoutSeconds); // cap to your configured vis timeout
                                try
                                {
                                    if (!ok && _dedupOpts.Enabled)
                                        _dedup.Release(msg.MessageId);
                                    await sqsClient.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                                    {
                                        QueueUrl = _sqs.QueueUrl,
                                        ReceiptHandle = msg.ReceiptHandle,
                                        VisibilityTimeout = extendBySeconds
                                    }, stoppingToken);
                                   
                                    _logger.LogWarning(
                                        "Kafka failed for message {Id}. Not deleting from SQS. ReceiveCount={ReceiveCount}. Extended visibility by {Seconds}s.",
                                        msg.MessageId, receiveCount, extendBySeconds);
                                }
                                catch (Exception ex2)
                                {
                                    _logger.LogError(ex2,
                                        "Failed to extend visibility for message {Id}. It may reappear soon. ReceiveCount={ReceiveCount}",
                                        msg.MessageId, receiveCount);
                                }

                                // (We’ll add retries / DLQ handling next.)
                            }*/

                        }
                    }
                    else
                    {
                        await Task.Delay(_sqs.EmptyWaitDelayMs, stoppingToken);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error while polling SQS");
                    await Task.Delay(2000, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // swallow: host is stopping

        }
        _logger.LogInformation("ExecuteAsync exiting.");
    }


    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping worker… flushing Kafka producer.");
        try
        {
            await _producer.DisposeAsync(); // your IKafkaProducer flushes & disposes
            _logger.LogInformation("Kafka producer closed.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error while closing Kafka producer.");
        }

        await base.StopAsync(cancellationToken);
    }


}


//await sqsClient.DeleteMessageAsync(_sqs.QueueUrl, msg.ReceiptHandle, stoppingToken);
//_logger.LogInformation("Deleted message {Id} from SQS.", msg.MessageId);