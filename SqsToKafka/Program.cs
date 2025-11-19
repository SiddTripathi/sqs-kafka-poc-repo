using Microsoft.Extensions.DependencyInjection;
using SqsToKafka;
using SqsToKafka.Options;
using SqsToKafka.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;                 // ValidateOnStart
using SqsToKafka.Services.Dedup;
using SqsToKafka.Replay;


Host.CreateDefaultBuilder(args)
    .ConfigureHostOptions(o =>
    {
        o.ShutdownTimeout = TimeSpan.FromSeconds(20); // gives time to flush Kafka on SIGTERM
    })
    .ConfigureServices((context, services) =>
    {
        var cfg = context.Configuration;

        // --Options binding + validation -----

        services.AddOptions<KafkaOptions>()
                .Bind(cfg.GetSection("kafka"))
                .ValidateDataAnnotations()
                .ValidateOnStart();
        services.AddOptions<SqsOptions>()
                .Bind(cfg.GetSection("Sqs"))
                .ValidateDataAnnotations()
                .ValidateOnStart();
        services.AddOptions<RoutingOptions>()
                .Bind(cfg.GetSection("Routing"))
                .ValidateDataAnnotations()
                .ValidateOnStart();
        services.AddOptions<DedupOptions>()
                .Bind(cfg.GetSection("dedup"))
                .ValidateDataAnnotations()
                .ValidateOnStart();
        services.AddOptions<DedupSqlOptions>()
                .Bind(cfg.GetSection("DedupSql"))
                .ValidateOnStart();
        services.AddOptions<RetryOptions>()
                .Bind(cfg.GetSection("Retry"))
                .ValidateDataAnnotations()
                .ValidateOnStart();
        services.AddOptions<VisibilityOptions>()
                .Bind(cfg.GetSection("Visibility"))
                .ValidateOnStart();
        services.AddOptions<DeadLetterOptions>()
                .Bind(cfg.GetSection("DeadLetter"))
                .ValidateOnStart();
        // --- Replay (Blob recording) ----
        services.AddOptions<ReplayOptions>()
                .Bind(cfg.GetSection("Replay"))
                .ValidateOnStart();

        // --- Services ----
        services.AddHostedService<SqsToKafkaWorker>();
        services.AddSingleton<IKafkaProducer, KafkaProducer>();
        services.AddSingleton<IDedupCache, InMemoryDedupCache>();
        // Replay store (Blob-based)
        services.AddSingleton<IReplayStore>(sp =>
        {
            var opts = sp.GetRequiredService<IOptions<ReplayOptions>>().Value;
            return new BlobReplayStore(opts);
        });


        services.AddSingleton<IDedupStore>(sp =>
        {
            var o = sp.GetRequiredService<IOptions<DedupSqlOptions>>().Value;
            if (o.Enabled && !string.IsNullOrWhiteSpace(o.ConnectionString))
                return new SqlDedupStore(o.ConnectionString!, o.TtlDays);

            return new NoopDedupStore();
        });


    })
    .Build()
    .Run();
