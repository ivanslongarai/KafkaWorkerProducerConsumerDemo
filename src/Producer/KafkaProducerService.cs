using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Producer;

public sealed class KafkaProducerService : BackgroundService
{
    private readonly ILogger<KafkaProducerService> _log;
    private readonly KafkaOptions _opt;

    public KafkaProducerService(
        ILogger<KafkaProducerService> log,
        IOptions<KafkaOptions> opt)
    {
        _log = log;
        _opt = opt.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _opt.BootstrapServers
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();
        _log.LogInformation("Producer started. Bootstrap={Bootstrap}", _opt.BootstrapServers);

        for (var i = 0; i < 300 && !stoppingToken.IsCancellationRequested; i++)
        {
            try
            {
                var key = $"Key_{i}";
                var value = Guid.NewGuid().ToString();

                var deliveryReport = await producer.ProduceAsync(
                    _opt.Topic,
                    new Message<string, string> { Key = key, Value = value },
                    stoppingToken
                );

                _log.LogInformation(
                    "Delivered message: Key={Key} | Value={Value} | TopicPartitionOffset={TPO}",
                    key, value, deliveryReport.TopicPartitionOffset
                );

                await Task.Delay(100, stoppingToken); // small delay just to slow down demo output
            }
            catch (ProduceException<string, string> ex)
            {
                _log.LogError(ex, "Delivery failed: {Reason}", ex.Error.Reason);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        producer.Flush(TimeSpan.FromSeconds(5));
        _log.LogInformation("Producer stopped gracefully.");
    }
}
