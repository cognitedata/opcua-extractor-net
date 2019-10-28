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
using System.Collections.Generic;
using System.ComponentModel;
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
        [InlineData(CDFMockHandler.MockMode.All, "basic")]
        [InlineData(CDFMockHandler.MockMode.Some, "basic")]
        [InlineData(CDFMockHandler.MockMode.None, "basic")]
        [InlineData(CDFMockHandler.MockMode.FailAsset, "basic")]
        [InlineData(CDFMockHandler.MockMode.All, "full")]
        [InlineData(CDFMockHandler.MockMode.Some, "full")]
        [InlineData(CDFMockHandler.MockMode.None, "full")]
        [InlineData(CDFMockHandler.MockMode.FailAsset, "full")]
        public async Task TestBasicPushing(CDFMockHandler.MockMode mode, string serverType)
        {
            var fullConfig = Common.BuildConfig(serverType, 3);
            Logger.Configure(fullConfig.Logging);
            Log.Information("Starting OPC UA Extractor version {version}", Cognite.OpcUa.Version.GetVersion());
            Log.Information("Revision information: {status}", Cognite.OpcUa.Version.Status());
            Log.Information("Testing with MockMode {TestBasicPushingMockMode}", mode.ToString());
            var client = new UAClient(fullConfig);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            var handler = new CDFMockHandler(config.Project, mode);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);
            var extractor = new Extractor(fullConfig, pusher, client);
            try
            {
                await extractor.RunExtractor(CancellationToken.None, true);
            }
            catch (Exception e)
            {
                if (mode != CDFMockHandler.MockMode.FailAsset)
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
            Logger.Configure(fullConfig.Logging);
            var client = new UAClient(fullConfig);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            var runTask = extractor.RunExtractor(source.Token);

            File.Create(config.BufferFile).Close();
            handler.AllowPush = false;
            bool gotData = false;
            for (int i = 0; i < 40; i++)
            {
                if (new FileInfo(config.BufferFile).Length > 0)
                {
                    gotData = true;
                    break;
                }
                Thread.Sleep(500);
            }
            Assert.True(gotData, "Some data must be written");
            handler.AllowPush = true;
            gotData = false;
            for (int i = 0; i < 40; i++)
            {
                if (new FileInfo(config.BufferFile).Length == 0)
                {
                    gotData = true;
                    break;
                }
                Thread.Sleep(500);
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
            var client = new UAClient(fullConfig);
            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            var runTask = extractor.RunExtractor(source.Token);
            bool started = false;
            for (int i = 0; i < 20; i++)
            {
                if (extractor.Pushing)
                {
                    started = true;
                    break;
                }
                Thread.Sleep(500);
            }
            Assert.True(started);
            source.Cancel();
            try
            {
                await runTask;
            }
            catch (Exception e)
            {
                if (!Common.TestRunResult(e)) throw;
            }
            Assert.Equal(0, handler.RequestCount);
            extractor.Close();
        }
        [Trait("Category", "arrayserver")]
        [Fact]
        public async Task TestArrayData()
        {
            var fullConfig = Common.BuildConfig("array", 6);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            fullConfig.Extraction.AllowStringVariables = true;
            fullConfig.Extraction.MaxArraySize = 4;
            Logger.Configure(fullConfig.Logging);

            var client = new UAClient(fullConfig);
            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            bool gotData = false;
            var runTask = extractor.RunExtractor(source.Token);
            for (int i = 0; i < 20; i++)
            {
                if (handler.assets.Count == 4 && handler.timeseries.Count == 7)
                {
                    gotData = true;
                    break;
                }
                Thread.Sleep(500);
            }
            Assert.True(gotData, $"Expected to get 4 assets and got {handler.assets.Count}, 7 timeseries and got {handler.timeseries.Count}");
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
        [Trait("Category", "basicserver")]
        [Trait("Category", "restart")]
        [Fact]
        public async Task TestExtractorRestart()
        {
            var fullConfig = Common.BuildConfig("basic", 9);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            Logger.Configure(fullConfig.Logging);

            var client = new UAClient(fullConfig);
            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            bool started = false;
            var runTask = extractor.RunExtractor(source.Token);
            for (int i = 0; i < 20; i++)
            {
                if (extractor.Pushing)
                {
                    started = true;
                    break;
                }
                Thread.Sleep(500);
            }
            Assert.True(started);
            Assert.True(extractor.Started);
            extractor.RestartExtractor(source.Token);
            Thread.Sleep(500);
            started = false;
            for (int i = 0; i < 20; i++)
            {
                if (extractor.Started)
                {
                    started = true;
                    break;
                }
                Thread.Sleep(500);
            }
            Assert.True(started);
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
        [InlineData(20000, 100, 20, 1000, 100000)]
        [InlineData(200, 10000, 20, 10, 100000)]
        [InlineData(20000, 5, 2, 10000, 50000)]
        [Trait("Category", "DictChunk")]
        [Theory]
        public void TestDictionaryChunking(int timeseries, int datapoints, int expChunks, int expTimeseriesMax, int expDatapointsMax)
        {
            var dict = new Dictionary<string, List<BufferedDataPoint>>();
            for (int i = 0; i < timeseries; i++)
            {
                var points = new List<BufferedDataPoint>();
                for (int j = 0; j < datapoints; j++)
                {
                    points.Add(new BufferedDataPoint(DateTime.MinValue, "id" + i, i*datapoints + j));
                }

                dict["id" + i] = points;
            }
            var results = Utils.ChunkDictOfLists(dict, 100000, 10000);
            var min = results.Min(dct => dct.Values.Min(val => val.Count()));
            Assert.True(min > 0);
            var max = results.Max(dct => dct.Values.Sum(val => val.Count()));
            var maxTs = results.Max(dct => dct.Values.Count);
            Assert.Equal(expDatapointsMax, max);
            Assert.Equal(expTimeseriesMax, maxTs);
            Assert.Equal(expChunks, results.Count());
            var total = results.Sum(dct => dct.Values.Sum(val => val.Count()));
            var totalTs = results.Sum(dct => dct.Values.Count);
            Assert.Equal(timeseries, totalTs);
            Assert.Equal(datapoints * timeseries, total);

            var exists = new bool[timeseries * datapoints];
            foreach (var dct in results)
            {
                foreach (var kvp in dct)
                {
                    foreach (var dp in kvp.Value)
                    {
                        exists[(int) dp.DoubleValue] = true;
                    }
                }
            }
            Assert.True(exists.All(val => val));

        }
        [Fact]
        [Trait("Category", "connectiontest")]
        public async Task TestConnectionTest()
        {
            var fullConfig = Common.BuildConfig("basic", 9);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            Logger.Configure(fullConfig.Logging);

            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);
            var res = await pusher.TestConnection(CancellationToken.None);
            Log.CloseAndFlush();
            Assert.True(res);
        }
    }
}
