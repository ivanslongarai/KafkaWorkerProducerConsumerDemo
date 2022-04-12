using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Consumer
{
    public class ConsumerWorker : BackgroundService
    {
        private readonly ILogger<ConsumerWorker> _log;
        private readonly IConfiguration _config;
        private readonly string _host;
        private readonly string _topic;

        public ConsumerWorker(ILogger<ConsumerWorker> log, IConfiguration config)
        {
            _log = log;
            _config = config;
            _host = _config.GetSection("Kafka:Host").Value;
            _topic = _config.GetSection("Kafka:Topic").Value;
        }

        protected override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _host,
                GroupId = $"{_topic}-group-0",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<string, string>(config).Build())
            {
                consumer.Subscribe(_topic);
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumerResult = consumer.Consume(cancellationToken);
                        _log.LogInformation($"Key: {consumerResult.Message.Key} | Value: {consumerResult.Message.Value}");
                    }
                    catch (OperationCanceledException)
                    {
                        continue;
                    }
                }
            }
            return Task.CompletedTask;
        }
    }
}
