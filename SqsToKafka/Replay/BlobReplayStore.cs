using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Blobs.Models;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace SqsToKafka.Replay
{
    /// <summary>
    /// Stores replay records in Azure Blob Storage as single JSON documents.
    /// </summary>
    public class BlobReplayStore : IReplayStore
    {
        private readonly BlobContainerClient _container;
        private readonly ReplayOptions _options;
        private readonly JsonSerializerOptions _jsonOptions;

        public BlobReplayStore(ReplayOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));

            // When replay is disabled, BlobReplayStore becomes a no-op.
            if (!_options.Enabled)
            {
                _container = null!;
                _jsonOptions = new JsonSerializerOptions();
                return;
            }

            if (string.IsNullOrWhiteSpace(options.ConnectionString))
                throw new InvalidOperationException("ReplayOptions.ConnectionString is required when Replay.Enabled=true.");

            if (string.IsNullOrWhiteSpace(options.ContainerName))
                throw new InvalidOperationException("ReplayOptions.ContainerName is required when Replay.Enabled=true.");

            _container = new BlobContainerClient(options.ConnectionString, options.ContainerName);

            _jsonOptions = new JsonSerializerOptions
            {
                WriteIndented = false,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };
        }

        public async Task RecordAsync(ReplayRecord record, CancellationToken cancellationToken)
        {
            //This ensures we never accidentally hit Azure SDK when disabled.

            if (!_options.Enabled || _container == null)
                return;

            if (record == null)
                return;

            // Ensure container exists
            await _container.CreateIfNotExistsAsync(cancellationToken: cancellationToken);

            // Build time-based prefix
            var ts = record.RecordedAtUtc;
            string prefix =
                $"{_options.BasePath}/" +
                $"{ts:yyyy}/{ts:MM}/{ts:dd}/{ts:HH}/";

            // Safe filename: timestamp + dedupid + sqs message id
            string fileName =
                $"{ts:yyyy-MM-ddTHH-mm-ss.fffZ}__{record.DedupId ?? "no-dedup"}__{record.SqsMessageId ?? "no-id"}.json";

            string blobPath = prefix + fileName;

            // Serialize record
            byte[] json = JsonSerializer.SerializeToUtf8Bytes(record, _jsonOptions);

            // Upload using block blob client
            BlobClient blob = _container.GetBlobClient(blobPath);

            using var stream = new System.IO.MemoryStream(json);
            await blob.UploadAsync(stream, overwrite: true, cancellationToken: cancellationToken);
        }
    }
}
