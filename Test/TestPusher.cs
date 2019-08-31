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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Serilog;
using Xunit;

namespace Test
{
    public class TestPusher : IPusher
    {
        public Extractor Extractor { private get; set; }
        public UAClient UAClient { private get; set; }
        public Action EndCB { private get; set; }
        public PusherConfig BaseConfig { get; private set; }

        public ConcurrentQueue<BufferedDataPoint> BufferedDPQueue { get; } = new ConcurrentQueue<BufferedDataPoint>();

        private readonly Dictionary<string, Action<List<BufferedNode>, List<BufferedVariable>, List<BufferedVariable>>> nodeTests;
        private readonly Action<List<BufferedDataPoint>> dpTest;

        public TestPusher(Dictionary<string, Action<List<BufferedNode>, List<BufferedVariable>, List<BufferedVariable>>> nodeTests,
            Action<List<BufferedDataPoint>> dpTest)
        {
            this.nodeTests = nodeTests;
            this.dpTest = dpTest;
            BaseConfig = new CogniteClientConfig() { DataPushDelay = 1000, Debug = false };
        }

        private void SyncPushDps(ConcurrentQueue<BufferedDataPoint> dataPointQueue)
        {
            var dataPointList = new List<BufferedDataPoint>();
            int count = 0;
            while (dataPointQueue.TryDequeue(out BufferedDataPoint buffer) && count++ < 100000)
            {
                Assert.True(buffer.timestamp > DateTime.MinValue, "Invalid timestamp");
                dataPointList.Add(buffer);
            }
            Log.Information("Got {NumDummyDatapoints} datapoints", count);
            dpTest?.Invoke(dataPointList);
        }
        public async Task PushDataPoints(CancellationToken token)
        {
            await Task.Run(() => SyncPushDps(BufferedDPQueue));
        }
        private void SyncPushNodes(IEnumerable<BufferedNode> nodes, IEnumerable<BufferedVariable> variables, CancellationToken token)
        {
            var histTsList = new List<BufferedVariable>();
            var tsList = new List<BufferedVariable>();

            foreach (var node in variables)
            {
                if (node.IsProperty) continue;
                if (Extractor.AllowTSMap(node))
                {
                    if (node.Historizing)
                    {
                        histTsList.Add(node);
                    }
                    else
                    {
                        tsList.Add(node);
                    }
                }
            }
            nodeTests?.GetValueOrDefault("afterdata")?.Invoke(nodes.ToList(), tsList, histTsList);
            UAClient.GetNodeProperties(nodes.Concat(tsList).Concat(histTsList), token);
            nodeTests?.GetValueOrDefault("afterProperties")?.Invoke(nodes.ToList(), tsList, histTsList);
            Extractor.SynchronizeNodes(tsList.Concat(histTsList), token);
            nodeTests?.GetValueOrDefault("afterSynchronize")?.Invoke(nodes.ToList(), tsList, histTsList);
            EndCB?.Invoke();
        }
        public async Task<bool> PushNodes(IEnumerable<BufferedNode> nodes, IEnumerable<BufferedVariable> variables, CancellationToken token)
        {
            await Task.Run(() => SyncPushNodes(nodes, variables, token));
            return true;
        }

        public void Reset()
        {
        }
    }
}
