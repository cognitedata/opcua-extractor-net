/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Opc.Ua;
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

            await tester.TerminateRunTask(false, e =>
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
            tester.Config.Source.RestartOnReconnect = true;

            await tester.StartServer();

            using var process = CommonTestUtils.GetProxyProcess(4839, 62546);
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

            await tester.TerminateRunTask(false);
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
            tester.Config.Source.RestartOnReconnect = true;

            await tester.StartServer();

            using var process = CommonTestUtils.GetProxyProcess(4839, 62546);
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

            await tester.TerminateRunTask(false, ex =>
                ex is TaskCanceledException || ex is AggregateException aex && aex.InnerException is TaskCanceledException);
        }
        [Trait("Server", "wrong")]
        [Trait("Target", "UAExtractor")]
        [Trait("Test", "updatenullproperty")]
        [Fact]
        public async Task TestUpdateNullPropertyValue()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Wrong
            });
            await tester.ClearPersistentData();
            await tester.StartServer();

            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.Extraction.Update = new UpdateConfig
            {
                Objects = new TypeUpdateConfig
                {
                    Metadata = true
                },
                Variables = new TypeUpdateConfig
                {
                    Metadata = true
                }
            };

            tester.StartExtractor();

            await tester.Extractor.WaitForSubscriptions();

            Assert.True(string.IsNullOrEmpty(tester.Handler.Assets["gp.tl:i=2"].metadata["TooLargeDim"]));

            await tester.Extractor.Rebrowse();

            Assert.True(string.IsNullOrEmpty(tester.Handler.Assets["gp.tl:i=2"].metadata["TooLargeDim"]));

            tester.Server.Server.MutateNode(tester.Server.Ids.Wrong.TooLargeProp, state =>
            {
                var varState = state as PropertyState;
                varState.ArrayDimensions = new ReadOnlyList<uint>(new List<uint> { 5 });
                varState.Value = Enumerable.Range(0, 5).ToArray();
            });

            await tester.Extractor.Rebrowse();

            Assert.Equal("[0, 1, 2, 3, 4]", tester.Handler.Assets["gp.tl:i=2"].metadata["TooLargeDim"]);

            await tester.TerminateRunTask(false);
        }
    }
}
