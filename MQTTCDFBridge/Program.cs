using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

[assembly: CLSCompliant(false)]
namespace Cognite.Bridge
{
    internal sealed class Program
    {
        private static void Main(string[] args)
        {
            var configPath = Environment.GetEnvironmentVariable("MQTT_BRIDGE_CONFIG_DIR");
            if (configPath == null)
            {
                configPath = args.Length > 0 ? args[0] : "config/config.bridge.yml";
            }
            var services = new ServiceCollection();
            var config = services.AddConfig<BridgeConfig>(configPath, 1);
            services.AddCogniteClient("MQTT-CDF Bridge");
            services.AddLogger();
            using var provider = services.BuildServiceProvider();
            RunBridge(config, provider).Wait();
        }
        /// <summary>
        /// Public to expose for tests, start the bridge with given configuration and HTTP-Client configured serviceprovider.
        /// </summary>
        /// <param name="config">Full configuration to use</param>
        /// <param name="provider">HTTP-Client configured serviceprovider</param>
        /// <returns></returns>
        public static async Task RunBridge(BridgeConfig config, IServiceProvider provider)
        {
            var destination = new Destination(config.Cognite, provider);
            var log = provider.GetRequiredService<ILogger<MQTTBridge>>();

            using var bridge = new MQTTBridge(destination, config, log);

            using var source = new CancellationTokenSource();

            using var quitEvent = new ManualResetEvent(false);
            Console.CancelKeyPress += (sender, eArgs) =>
            {
                quitEvent?.Set();
                eArgs.Cancel = true;
                source?.Cancel();
            };

            while (!source.Token.IsCancellationRequested)
            {
                try
                {
                    await bridge.StartBridge(source.Token);
                    break;
                }
                catch (Exception ex)
                {
                    log.LogWarning("Failed to start bridge: {Message}, retrying", ex.Message);
                }

                await Task.Delay(2000, source.Token);
            }

            quitEvent.WaitOne();

            await Serilog.Log.CloseAndFlushAsync();
        }
    }
}
