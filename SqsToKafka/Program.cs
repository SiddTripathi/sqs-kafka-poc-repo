using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SqsToKafka;
using SqsToKafka.Options;
using SqsToKafka.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;                 // ValidateOnStart
using SqsToKafka.Options;
using SqsToKafka.Services.Dedup;


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
        
        // --- Services ----
        services.AddHostedService<SqsToKafkaWorker>();
        services.AddSingleton<IKafkaProducer, KafkaProducer>();
        services.AddSingleton<IDedupCache, InMemoryDedupCache>();
       /* var dedupMode = cfg.GetValue<string>("dedup:mode")?.ToLowerInvariant() ?? "memory";
        if (dedupMode == "sql")
        {
            services.AddSingleton<IDedupCache, SqlDedupCache>();
        }
        else
        {
            services.AddSingleton<IDedupCache, InMemoryDedupCache>();
        }
       */


    })
    .Build()
    .Run();
