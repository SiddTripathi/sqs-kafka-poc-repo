using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqsToKafka.Replay;
using SqsToKafka.Options;
using SqsToKafka.Services;
using SqsToKafka.Services.Dedup;
using System.Text.Json;
using System.Linq;




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
    private readonly RetryOptions _retry;
    private readonly VisibilityOptions _vis;
    private readonly DeadLetterOptions _dlq;
    private readonly IReplayStore _replay;


    public SqsToKafkaWorker(
        ILogger<SqsToKafkaWorker> logger,
        IOptions<KafkaOptions> kafka,
        IOptions<SqsOptions> sqs,
        IKafkaProducer producer,
        IOptions<RoutingOptions> routing,
        IDedupCache dedup,
        IOptions<DedupOptions> dedupOptions,
        IDedupStore dedupStore,
        IOptions<RetryOptions> retry,
        IOptions<VisibilityOptions> vis,
        IOptions<DeadLetterOptions> dlq,
        IReplayStore replayStore)
    {
        _logger = logger;
        _kafka = kafka.Value;
        _sqs = sqs.Value;
        _producer = producer;
        _routing = routing.Value;
        _dedup = dedup;
        _dedupOpts = dedupOptions.Value;
        _dedupStore = dedupStore;
        _retry = retry.Value;
        _vis = vis.Value;
        _dlq = dlq.Value;
        _replay = replayStore;

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

    private TimeSpan ComputeBackoff(int attempt)
    {
        // attempt is 1-based (1,2,3...)
        var cap = Math.Min(_retry.MaxDelayMs, _retry.BaseDelayMs * (int)Math.Pow(2, attempt - 1));
        if (_retry.Jitter)
        {
            var jitter = Random.Shared.Next(0, (int)(cap * 0.2)); // up to +20%
            cap += jitter;
        }
        return TimeSpan.FromMilliseconds(cap);
    }

    private sealed class CancellationDisposable : IDisposable
    {
        private readonly CancellationTokenSource _cts;
        public CancellationDisposable(CancellationTokenSource cts) => _cts = cts;
        public void Dispose() => _cts.Cancel();
    }

    private IDisposable StartVisibilityAutoExtend(AmazonSQSClient client, string queueUrl, string receiptHandle, CancellationToken parent)
    {
        // If disabled, return a no-op disposable
        if (!_vis.AutoExtend) return new CancellationDisposable(new CancellationTokenSource(0));

        var cts = CancellationTokenSource.CreateLinkedTokenSource(parent);
        var step = TimeSpan.FromSeconds(Math.Max(5, _vis.StepSeconds));
        var deadline = DateTimeOffset.UtcNow.AddSeconds(Math.Max(_vis.StepSeconds, _vis.MaxPerMessageSeconds));

        _ = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested && DateTimeOffset.UtcNow < deadline)
            {
                try
                {
                    await Task.Delay(step, cts.Token);

                    // Renew to slightly longer than the step so we always stay ahead
                    var newTimeout = (int)Math.Min(_sqs.VisibilityTimeoutSeconds, step.TotalSeconds + 10);

                    await client.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                    {
                        QueueUrl = queueUrl,
                        ReceiptHandle = receiptHandle,
                        VisibilityTimeout = newTimeout
                    }, cts.Token);

                    _logger.LogDebug("Auto-extended SQS visibility by ~{Seconds}s for DedupId/Msg: {Id}",
                        newTimeout, receiptHandle);
                }
                catch (OperationCanceledException) { /* stopping */ }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Auto-extend failed for receiptHandle={Receipt}", receiptHandle);
                }
            }
        }, cts.Token);

        return new CancellationDisposable(cts);
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
                            // Compute the dedup ID: prefer attribute/body "DedupId", else SQS MessageId
                            string dedupId = msg.MessageId;

                            // 1) Attribute: DedupId -- This is only to test the in memory and sql dedup logic. In actual scenario the sqs msg id will be used.
                            if (msg.MessageAttributes != null &&
                                msg.MessageAttributes.TryGetValue("DedupId", out var didAttr) &&
                                !string.IsNullOrWhiteSpace(didAttr.StringValue))
                            {
                                dedupId = didAttr.StringValue!;
                            }
                            else
                            {
                                // 2) Body JSON: { "DedupId": "..." }
                                try
                                {
                                    using var doc2 = JsonDocument.Parse(msg.Body);
                                    if (doc2.RootElement.ValueKind == JsonValueKind.Object &&
                                        doc2.RootElement.TryGetProperty("DedupId", out var didBody) &&
                                        didBody.ValueKind == JsonValueKind.String)
                                    {
                                        var val = didBody.GetString();
                                        if (!string.IsNullOrWhiteSpace(val)) dedupId = val!;
                                    }
                                }
                                catch { /* non-JSON or missing — ignore */ }
                            }

                            if (await _dedupStore.SeenBeforeAsync(dedupId, stoppingToken))
                            {
                                _logger.LogWarning("[Dedup:SQL] Already processed DedupId={DedupId} — deleting redelivery.", dedupId);
                                // Do NOT delete from SQS here; let redrive/visibility policies handle it if needed.
                                // If you do not WANT to delete already-seen messages, comment:
                                await sqsClient.DeleteMessageAsync(_sqs.QueueUrl, msg.ReceiptHandle, stoppingToken);
                                continue;
                            }


                            if (_dedupOpts.Enabled)
                            {
                                var ttl = TimeSpan.FromSeconds(_dedupOpts.TtlSeconds);
                                if (!_dedup.TryAcquire(dedupId, ttl))
                                {
                                    _logger.LogWarning("[Dedup] Duplicate message skipped: {DedupId}", dedupId);
                                    //await sqsClient.DeleteMessageAsync(_sqs.QueueUrl, msg.ReceiptHandle, stoppingToken);
                                    continue;
                                }
                            }
                            var (topic, key) = ResolveRouting(msg);
                            //-----Replay Logic -------------

                            string? eventType = null;
                            try
                            {
                                using var bodyDoc = JsonDocument.Parse(msg.Body);
                                if (bodyDoc.RootElement.ValueKind == JsonValueKind.Object &&
                                    bodyDoc.RootElement.TryGetProperty("event", out var evProp) &&
                                    evProp.ValueKind == JsonValueKind.String)
                                {
                                    eventType = evProp.GetString();
                                }
                            }
                            catch
                            {
                                // Non-JSON body is fine; eventType stays null
                            }

                            var replayRecord = new ReplayRecord
                            {
                                Environment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "unknown",
                                RecordedAtUtc = DateTime.UtcNow,
                                Source = "sqs-to-kafka-worker",
                                PodName = null,   // we can wire K8s info later if needed
                                NodeName = null,

                                SqsMessageId = msg.MessageId,
                                SqsReceiptHandle = msg.ReceiptHandle,
                                SqsAttributes = msg.Attributes,
                                MessageAttributes = msg.MessageAttributes?
                                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value.StringValue ?? string.Empty),

                                BodyJson = msg.Body,
                                EventType = eventType,
                                DedupId = dedupId,
                                KafkaKey = key,

                                TargetTopic = topic,
                                DefaultTopic = _kafka.DefaultTopic,
                                DlqTopic = _dlq.Topic,
                                Status = ReplayStatus.Processed
                            };

                            await _replay.RecordAsync(replayRecord, stoppingToken);

                            //-------End Replay-----

                            var attempt = 0;
                            //Exception? lastError = null;
                            var produced = false;
                            // Start auto-renewal of visibility while processing this message
                            using var visLease = StartVisibilityAutoExtend(sqsClient, _sqs.QueueUrl, msg.ReceiptHandle, stoppingToken);
                            //The below line is for test only to test the extended visibility feature locally. 
                            //await Task.Delay(TimeSpan.FromSeconds(90), stoppingToken);

                            while (attempt < _retry.MaxProduceAttempts && !stoppingToken.IsCancellationRequested)
                            {
                                attempt++;
                                try
                                {
                                    // 1) Produce to Kafka
                                    produced = await _producer.ProduceAsync(topic, key, msg.Body, stoppingToken);

                                    if (produced)
                                    {
                                        //_dedup.Release(msg.MessageId);
                                        if (attempt > 1)
                                            _logger.LogInformation("Produce succeeded on attempt {Attempt} for {DedupId}.", attempt, dedupId);
                                        await sqsClient.DeleteMessageAsync(_sqs.QueueUrl, msg.ReceiptHandle, stoppingToken);
                                        _logger.LogInformation("Deleted message {Id} from SQS after Kafka ack.", dedupId);
                                        await _dedupStore.MarkProcessedAsync(dedupId, DateTimeOffset.UtcNow, stoppingToken);
                                        break;
                                        
                                    }
                                    else
                                    {
                                        _logger.LogWarning("Kafka failed for message {Id}. Not deleting from SQS.", dedupId);

                                        // Allow retry: free the in-flight lock
                                        if (_dedupOpts.Enabled) _dedup.Release(dedupId);

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
                                                rc, extendBySeconds, dedupId);
                                        }
                                        catch (Exception ex2)
                                        {
                                            _logger.LogError(ex2, "Failed to extend visibility for {Id}.", dedupId);
                                        }
                                        var delay = ComputeBackoff(attempt);
                                        _logger.LogWarning("Produce returned false (attempt {Attempt}/{Max}). Backing off {Delay}… DedupId={DedupId}",
                                            attempt, _retry.MaxProduceAttempts, delay, dedupId);
                                        await Task.Delay(delay, stoppingToken);

                                    }
                                }
                                catch (Exception ex)
                                {
                                    _logger.LogError(ex, "Error producing message {Id}.", dedupId);
                                    if (_dedupOpts.Enabled) _dedup.Release(dedupId);  // free lock on unexpected error
                                    var delay = ComputeBackoff(attempt);
                                    _logger.LogWarning(ex, "Produce failed (attempt {Attempt}/{Max}). Backing off {Delay}… DedupId={DedupId}",
                                        attempt, _retry.MaxProduceAttempts, delay, dedupId);

                                    await Task.Delay(delay, stoppingToken);
                                }

                            }
                            if (!produced && !stoppingToken.IsCancellationRequested)
                            {
                                // 1) Try to publish a DLQ event (if enabled)
                                if (_dlq.Enabled)
                                {
                                    try
                                    {
                                        // Build DLQ payload (include original context safely)
                                        var dlqPayload = new
                                        {
                                            reason = "max_attempts_exceeded",
                                            attempts = _retry.MaxProduceAttempts,
                                            original = new
                                            {
                                                dedupId,
                                                sqsMessageId = msg.MessageId,
                                                topic = topic,
                                                key = key,
                                                // optionally include body/attributes based on options
                                                body = _dlq.IncludeBody ? msg.Body : null,
                                                attributes = _dlq.IncludeAttributes ? msg.MessageAttributes : null
                                            },
                                            failedAtUtc = DateTimeOffset.UtcNow
                                        };

                                        var dlqJson = JsonSerializer.Serialize(dlqPayload);

                                        // NOTE: we use the same key (if any) so DLQ can be partition-aware
                                        var dlqOk = await _producer.ProduceAsync(_dlq.Topic, key, dlqJson, stoppingToken);

                                        if (dlqOk)
                                        {
                                            _logger.LogError("Published to DLQ topic {Topic} for DedupId={DedupId} after {Attempts} attempts.",
                                                _dlq.Topic, dedupId, _retry.MaxProduceAttempts);

                                            // 2) Since DLQ captured it, delete from SQS to stop infinite redrives
                                            await sqsClient.DeleteMessageAsync(_sqs.QueueUrl, msg.ReceiptHandle, stoppingToken);
                                            _logger.LogWarning("Deleted message from SQS after DLQ handoff. DedupId={DedupId}", dedupId);
                                        }
                                        else
                                        {
                                            _logger.LogError("Failed to publish to DLQ topic {Topic}. Leaving message for SQS redrive. DedupId={DedupId}",
                                                _dlq.Topic, dedupId);
                                            // do NOT delete from SQS; let SQS redrive (and SQS DLQ) handle it
                                        }
                                    }
                                    catch (Exception exDlq)
                                    {
                                        _logger.LogError(exDlq, "DLQ publish threw an exception. Leaving message for SQS redrive. DedupId={DedupId}",
                                            dedupId);
                                        // do NOT delete from SQS; let SQS redrive (and SQS DLQ) handle it
                                    }
                                }
                                else
                                {
                                    // DLQ disabled → leave message for SQS redrive / SQS DLQ
                                    _logger.LogError("Produce failed after {Max} attempts for {DedupId}. DLQ disabled — leaving for SQS redrive.",
                                        _retry.MaxProduceAttempts, dedupId);
                                }

                                // End processing this message
                                continue;
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