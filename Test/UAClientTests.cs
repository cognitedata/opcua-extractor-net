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

        [Trait("Server", "basic")]
        [Trait("Target", "UAClient")]
        [Trait("Test", "connectingfailure")]
        [Fact]
        public async Task TestConnectionFailure()
        {
            using var tester = new ExtractorTester(new TestParameters
            {
                QuitAfterMap = true
            });
            await tester.ClearPersistentData();
            tester.Config.Source.EndpointURL = "opc.tcp://localhost:4000";

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
                    silent = Utils.GetRootSilentException(aex);
                }

                return silent != null && silent.Operation == Utils.SourceOp.SelectEndpoint;
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
            using var tester = new ExtractorTester(new TestParameters
            {
                HistoryGranularity = granularity
            });
            await tester.ClearPersistentData();

            tester.Config.History.DataChunk = 10000;

            tester.StartExtractor();

            await tester.WaitForCondition(() => (int) Common.GetMetricValue("opcua_history_reads") == expectedReads, 20,
                () => $"Expected history to be read {expectedReads} times, got {Common.GetMetricValue("opcua_history_reads")}");

            await tester.TerminateRunTask();
        }
    }
}
