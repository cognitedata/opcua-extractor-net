using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;

namespace Cognite.Bridge
{
    class Program
    {
        private static readonly ILogger log = Log.Logger.ForContext(typeof(Program));

        static void Main(string[] args)
        {
            var configPath = args.Length > 0 ? args[0] : "config/config.bridge.yml";
            var services = new ServiceCollection();
            var config = services.AddConfig<BridgeConfig>(configPath, 1);
            services.AddCogniteClient("MQTT-CDF Bridge");
            using var provider = services.BuildServiceProvider();
            LoggingUtils.Configure(config.Logger);
            log.Information(config.ToString());
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
            using var bridge = new MQTTBridge(destination, config);

            using var source = new CancellationTokenSource();

            using var quitEvent = new ManualResetEvent(false);
            Console.CancelKeyPress += (sender, eArgs) =>
            {
                quitEvent?.Set();
                eArgs.Cancel = true;
                source?.Cancel();
            };

            while (true)
            {
                try
                {
                    await bridge.StartBridge(source.Token);
                    break;
                }
                catch (Exception ex)
                {
                    log.Warning("Failed to start bridge: {msg}, retrying", ex.Message);
                }

                await Task.Delay(2000);
            }

            quitEvent.WaitOne();

            Log.CloseAndFlush();
        }
    }
}
