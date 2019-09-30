/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Serilog;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [CollectionDefinition("Pusher_tests", DisableParallelization = true)]
    public class CDFPusherTests : MakeConsoleWork
    {
        public CDFPusherTests(ITestOutputHelper output) : base(output) { }
        [Trait("Category", "both")]
        [Trait("Tests", "cdfpusher")]
        [Theory]
        [InlineData(DummyFactory.MockMode.All, "basic")]
        [InlineData(DummyFactory.MockMode.Some, "basic")]
        [InlineData(DummyFactory.MockMode.None, "basic")]
        [InlineData(DummyFactory.MockMode.FailAsset, "basic")]
        [InlineData(DummyFactory.MockMode.All, "full")]
        [InlineData(DummyFactory.MockMode.Some, "full")]
        [InlineData(DummyFactory.MockMode.None, "full")]
        [InlineData(DummyFactory.MockMode.FailAsset, "full")]
        public async Task TestBasicPushing(DummyFactory.MockMode mode, string serverType)
        {
            var fullConfig = Common.BuildConfig(serverType, 3);
            Logger.Configure(fullConfig.Logging);
            Log.Information("Starting OPC UA Extractor version {version}", Cognite.OpcUa.Version.GetVersion());
            Log.Information("Revision information: {status}", Cognite.OpcUa.Version.Status());
            Log.Information("Testing with MockMode {TestBasicPushingMockMode}", mode.ToString());
            UAClient client = new UAClient(fullConfig);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            var factory = new DummyFactory(config.Project, mode);
            var pusher = new CDFPusher(Common.GetDummyProvider(factory), config);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            try
            {
                await extractor.RunExtractor(CancellationToken.None, true);
            }
            catch (Exception e)
            {
                if (mode != DummyFactory.MockMode.FailAsset)
                {
                    if (!Common.TestRunResult(e)) throw;
                }
            }
            extractor.Close();
        }
        [Trait("Category", "basicserver")]
        [Trait("Tests", "cdfpusher")]
        [Trait("Tests", "autobuffer")]
        [Fact]
        public async Task TestAutoBuffering()
        {
            var fullConfig = Common.BuildConfig("basic", 4);
            if (fullConfig == null)
            {
                throw new Exception("No config");
            }
            Logger.Configure(fullConfig.Logging);
            UAClient client = new UAClient(fullConfig);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            var factory = new DummyFactory(config.Project, DummyFactory.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(factory), config);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            using (var source = new CancellationTokenSource())
            {
                var runTask = extractor.RunExtractor(source.Token);

                File.Create(config.BufferFile).Close();
                factory.AllowPush = false;
                bool gotData = false;
                for (int i = 0; i < 20; i++)
                {
                    if (new FileInfo(config.BufferFile).Length > 0)
                    {
                        gotData = true;
                        break;
                    }
                    Thread.Sleep(1000);
                }
                Assert.True(gotData, "Some data must be written");
                factory.AllowPush = true;
                gotData = false;
                for (int i = 0; i < 20; i++)
                {
                    if (new FileInfo(config.BufferFile).Length == 0)
                    {
                        gotData = true;
                        break;
                    }
                    Thread.Sleep(1000);
                }
                Assert.True(gotData, $"Expecting file to be emptied, but it contained {new FileInfo(config.BufferFile).Length} bytes of data");
                source.Cancel();
                try
                {
                    await runTask;
                }
                catch (Exception e)
                {
                    if (!Common.TestRunResult(e)) throw;
                }
                extractor.Close();
            }
        }
        [Trait("Tests", "basicserver")]
        [Trait("Tests", "cdfpusher")]
        [Fact]
        public async Task TestDebugMode()
        {
            var fullConfig = Common.BuildConfig("basic", 5);
            if (fullConfig == null) throw new Exception("No config");
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            config.Debug = true;
            config.ApiKey = null;

            Logger.Configure(fullConfig.Logging);
            UAClient client = new UAClient(fullConfig);
            var factory = new DummyFactory(config.Project, DummyFactory.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(factory), config);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            using (var source = new CancellationTokenSource())
            {
                var runTask = extractor.RunExtractor(source.Token);
                Thread.Sleep(2000);
                source.Cancel();
                try
                {
                    await runTask;
                }
                catch (Exception e)
                {
                    if (!Common.TestRunResult(e)) throw;
                }
                Assert.Equal(0, factory.RequestCount);
                extractor.Close();
            }
        }
        [Trait("Category", "ArrayServer")]
        [Fact]
        public async Task TestArrayData()
        {
            var fullConfig = Common.BuildConfig("array", 6);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            fullConfig.Extraction.AllowStringVariables = true;
            fullConfig.Extraction.MaxArraySize = 4;
            Logger.Configure(fullConfig.Logging);

            UAClient client = new UAClient(fullConfig);
            var factory = new DummyFactory(config.Project, DummyFactory.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(factory), config);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            using (var source = new CancellationTokenSource())
            {
                var gotData = false;
                var runTask = extractor.RunExtractor(source.Token);
                for (int i = 0; i < 10; i++)
                {
                    if (factory.assets.Count == 4 && factory.timeseries.Count == 7)
                    {
                        gotData = true;
                        break;
                    }
                    Thread.Sleep(1000);
                }
                Assert.True(gotData, $"Expected to get 4 assets and got {factory.assets.Count}, 7 timeseries and got {factory.timeseries.Count}");
                source.Cancel();
                try
                {
                    await runTask;
                }
                catch (Exception e)
                {
                    if (!Common.TestRunResult(e)) throw;
                }
                extractor.Close();
            }
        }
    }
}
