
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var build = new ConfigurationBuilder();

            build.SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables();

            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(build.Build())
                .WriteTo.Console()
                .CreateLogger();

            var app = Host.CreateDefaultBuilder()
                .ConfigureServices((context, services) =>
                {
                    services.AddHostedService<ConsumerWorker>();
                })
                .UseSerilog()
                .Build();

            app.Run();

        }
    }
}
