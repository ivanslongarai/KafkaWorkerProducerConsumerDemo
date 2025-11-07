using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Consumer;

public sealed class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _log;
    private readonly KafkaOptions _opt;

    public KafkaConsumerService(
        ILogger<KafkaConsumerService> log,
        IOptions<KafkaOptions> opt)
    {
        _log = log;
        _opt = opt.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _opt.BootstrapServers,
            GroupId = _opt.GroupId,
            EnableAutoCommit = _opt.EnableAutoCommit,
            AllowAutoCreateTopics = _opt.AllowAutoCreateTopics
        };

        if (Enum.TryParse<AutoOffsetReset>(_opt.AutoOffsetReset, true, out var autoReset))
            config.AutoOffsetReset = autoReset;
        else
            config.AutoOffsetReset = AutoOffsetReset.Earliest;

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) =>
            {
                _log.LogError("Kafka error: {Reason} | Code: {Code} | IsFatal: {IsFatal}",
                    e.Reason, e.Code, e.IsFatal);
            })
            .SetLogHandler((_, msg) =>
            {
                _log.LogDebug("Kafka log [{Level}] {Name} | {Message}",
                    msg.Level, msg.Name, msg.Message);
            })
            .Build();

        consumer.Subscribe(_opt.Topic);
        _log.LogInformation("Consumer started. Topic={Topic} | GroupId={GroupId} | Bootstrap={Bootstrap}",
            _opt.Topic, _opt.GroupId, _opt.BootstrapServers);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? cr = null;

                try
                {
                    cr = consumer.Consume(stoppingToken);
                    if (cr is null) continue;

                    // ---- Business logic ----
                    _log.LogInformation(
                        "Received | TPO={TPO} | Partition={Partition} | Offset={Offset} | Key={Key} | Value={Value}",
                        cr.TopicPartitionOffset, cr.Partition, cr.Offset, cr.Message.Key, cr.Message.Value);

                    await ProcessMessageAsync(cr, stoppingToken);

                    if ((bool)!config.EnableAutoCommit)
                    {
                        consumer.Commit(cr);
                        _log.LogDebug("Committed {TPO}", cr.TopicPartitionOffset);
                    }
                }
                catch (ConsumeException ex)
                {
                    _log.LogError(ex, "Consume error: {Reason}", ex.Error.Reason);
                    // Optionally: skip & commit poison messages, or route to a DLT
                    // if (cr != null && !config.EnableAutoCommit) consumer.Commit(cr);
                }
                catch (OperationCanceledException)
                {
                    break; // graceful shutdown
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Unexpected processing failure.");
                    await Task.Delay(200, stoppingToken); // avoid hot loop
                }
            }
        }
        finally
        {
            try
            {
                consumer.Close(); // leave group cleanly
                _log.LogInformation("Consumer closed gracefully.");
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Failed to close consumer cleanly.");
            }
        }
    }

    private static Task ProcessMessageAsync(ConsumeResult<string, string> cr, CancellationToken ct)
    {
        // Replace with your business logic
        return Task.CompletedTask;
    }
}
