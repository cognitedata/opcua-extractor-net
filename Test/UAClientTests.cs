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
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [Collection("Extractor tests")]
    public class UAClientTests : MakeConsoleWork 
    {
        public UAClientTests(ITestOutputHelper output) : base(output) { }

        [Trait("Server", "basic")]
        [Trait("Target", "UAClient")]
        [Trait("Test", "connectingfailure")]
        [Fact]
        public async Task TestConnectionFailure()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                QuitAfterMap = true
            });
            await tester.ClearPersistentData();
            tester.Config.Source.EndpointUrl = "opc.tcp://localhost:4000";

            tester.StartExtractor();

            await tester.WaitForCondition(() => tester.RunTask.IsFaulted, 20, "Expected run task to fail");

            await tester.TerminateRunTask(e =>
            {
                SilentServiceException silent = null;
                if (e is SilentServiceException silentEx)
                {
                    silent = silentEx;
                }
                else if (e is AggregateException aex)
                {
                    silent = ExtractorUtils.GetRootExceptionOfType<SilentServiceException>(aex);
                }

                return silent != null && silent.Operation == ExtractorUtils.SourceOp.SelectEndpoint;
            });
        }
        [Trait("Server", "basic")]
        [Trait("Target", "UAClient")]
        [Trait("Test", "granularity")]
        [Theory]
        [InlineData(1, 900)]
        [InlineData(3, 0)]
        public async Task TestHistoryReadGranularity(int expectedReads, int granularity)
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                HistoryGranularity = granularity
            });
            await tester.ClearPersistentData();
            await tester.StartServer();
            tester.Server.PopulateBaseHistory();

            tester.Config.Extraction.AllowStringVariables = true;
            tester.Config.History.DataChunk = 10000;

            tester.StartExtractor();

            await tester.WaitForCondition(() => (int) CommonTestUtils.GetMetricValue("opcua_history_reads") == expectedReads, 20,
                () => $"Expected history to be read {expectedReads} times, got {CommonTestUtils.GetMetricValue("opcua_history_reads")}");

            await tester.TerminateRunTask();
        }
        [Trait("Server", "basic")]
        [Trait("Target", "UAClient")]
        [Trait("Test", "reconnect")]
        [Fact]
        public async Task TestServerReconnect()
        {
            Assert.True(RuntimeInformation.IsOSPlatform(OSPlatform.Linux), "This test only runs on Linux");
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Proxy
            });
            tester.Config.History.Enabled = false;

            await tester.StartServer();

            using var process = CommonTestUtils.GetProxyProcess();
            process.Start();
            await tester.ClearPersistentData();

            await Task.Delay(500);

            tester.StartExtractor();

            await tester.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_extractor_starting", 0)
                                                && CommonTestUtils.TestMetricValue("opcua_connected", 1), 20,
                "Expected the extractor to finish startup");

            await tester.Extractor.Looper.WaitForNextPush();
            CommonTestUtils.StopProxyProcess();

            await tester.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_connected", 0), 20,
                "Expected client to disconnect");

            process.Start();

            await tester.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_connected", 1), 20,
                "Excpected client to reconnect");

            await tester.TerminateRunTask();
            CommonTestUtils.StopProxyProcess();
        }
        [Trait("Server", "basic")]
        [Trait("Target", "UAClient")]
        [Trait("Test", "disconnect")]
        [Fact]
        public async Task TestServerDisconnect()
        {
            Assert.True(RuntimeInformation.IsOSPlatform(OSPlatform.Linux), "This test only runs on Linux");
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Proxy
            });

            await tester.StartServer();

            using var process = CommonTestUtils.GetProxyProcess();
            process.Start();
            tester.Config.Source.ForceRestart = true;
            tester.Config.History.Enabled = false;

            await tester.ClearPersistentData();
            await Task.Delay(500);

            tester.StartExtractor();

            await tester.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_extractor_starting", 0)
                                                && CommonTestUtils.TestMetricValue("opcua_connected", 1), 20,
                "Expected the extractor to finish startup");
            await tester.Extractor.Looper.WaitForNextPush();
            CommonTestUtils.StopProxyProcess();


            await tester.WaitForCondition(() => tester.RunTask.IsCompleted, 20, "Expected runtask to terminate");

            await tester.TerminateRunTask(ex =>
                ex is ExtractorFailureException || ex is AggregateException aex && aex.InnerException is ExtractorFailureException);
        }
    }
}
