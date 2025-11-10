using System;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.Data.SqlClient;



namespace SqsToKafka.Services.Dedup
{
    public sealed class SqlDedupStore : IDedupStore
    {
        private readonly string _cs;
        private readonly int _ttlDays;

        public SqlDedupStore(string connectionString, int ttlDays)
        {
            _cs = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _ttlDays = ttlDays;
        }

        public async Task<bool> SeenBeforeAsync(string messageId, CancellationToken ct)
        {
            const string sql = "SELECT 1 FROM dbo.ProcessedMessage WITH (NOLOCK) WHERE MessageId=@id";
            await using var conn = new SqlConnection(_cs);
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.Add(new("@id", SqlDbType.NVarChar, 200) { Value = messageId });
            var res = await cmd.ExecuteScalarAsync(ct);
            return res is not null;
        }

        public async Task MarkProcessedAsync(string messageId, DateTimeOffset processedAt, CancellationToken ct)
        {
            const string upsert = @"
MERGE dbo.ProcessedMessage AS T
USING (SELECT @id AS MessageId, @ts AS ProcessedAtUtc) AS S
ON T.MessageId = S.MessageId
WHEN NOT MATCHED BY TARGET THEN
  INSERT (MessageId, ProcessedAtUtc) VALUES (S.MessageId, S.ProcessedAtUtc);";

            await using var conn = new SqlConnection(_cs);
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(upsert, conn);
            cmd.Parameters.Add(new("@id", SqlDbType.NVarChar, 200) { Value = messageId });
            cmd.Parameters.Add(new("@ts", SqlDbType.DateTime2) { Value = processedAt.UtcDateTime });
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task<int> CleanupAsync(CancellationToken ct)
        {
            const string sql = "DELETE FROM dbo.ProcessedMessage WHERE ProcessedAtUtc < DATEADD(day, -@ttl, SYSUTCDATETIME())";
            await using var conn = new SqlConnection(_cs);
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.Add(new("@ttl", SqlDbType.Int) { Value = _ttlDays });
            return await cmd.ExecuteNonQueryAsync(ct);
        }
    }

}
