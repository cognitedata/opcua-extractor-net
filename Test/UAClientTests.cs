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

        [Trait("Server", "basic")]
        [Trait("Target", "UAClient")]
        [Trait("Test", "connectingfailure")]
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
