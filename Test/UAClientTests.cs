using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [CollectionDefinition("UAClient_Tests", DisableParallelization = true)]
    public class UAClientTests : MakeConsoleWork 
    {
        public UAClientTests(ITestOutputHelper output) : base(output) { }

        [Trait("Category", "failure")]
        [Fact]
        public async Task TestConnectionFailure()
        {
            var fullConfig = Common.BuildConfig("basic", 12);
            // Some incorrect endpoint
            fullConfig.Source.EndpointURL = "opc.tcp://localhost:4000";
            Logger.Configure(fullConfig.Logging);
            var client = new UAClient(fullConfig);

            var extractor = new Extractor(fullConfig, new List<IPusher>(), client);
            using var source = new CancellationTokenSource();
            var runTask = extractor.RunExtractor(source.Token);

            for (int i = 0; i < 10; i++)
            {
                if (runTask.IsFaulted) break;
                await Task.Delay(1000);
            }
            Assert.True(runTask.IsFaulted);

            try
            {
                await runTask;
            }
            catch (Exception e)
            {
                SilentServiceException silent = null;
                if (e is SilentServiceException silentEx)
                {
                    silent = silentEx;
                } 
                else if (e is AggregateException aex)
                {
                    silent = Utils.GetRootSilentException(aex);
                }
                Assert.True(silent != null);
                Assert.True(silent.Operation == Utils.SourceOp.SelectEndpoint);
            }
            source.Cancel();
            extractor.Close();
        }
    }
}
