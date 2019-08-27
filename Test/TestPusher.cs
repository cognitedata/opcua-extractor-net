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
using Xunit;

namespace Test
{
    public class TestPusher : IPusher
    {
        public Extractor Extractor { private get; set; }
        public UAClient UAClient { private get; set; }
        public Action EndCB { private get; set; }

        public ConcurrentQueue<BufferedDataPoint> BufferedDPQueue { get; } = new ConcurrentQueue<BufferedDataPoint>();
        public ConcurrentQueue<BufferedNode> BufferedNodeQueue { get; } = new ConcurrentQueue<BufferedNode>();

        private readonly Dictionary<string, Action<List<BufferedNode>, List<BufferedVariable>, List<BufferedVariable>>> nodeTests;
        private readonly Action<List<BufferedDataPoint>> dpTest;

        public TestPusher(Dictionary<string, Action<List<BufferedNode>, List<BufferedVariable>, List<BufferedVariable>>> nodeTests,
            Action<List<BufferedDataPoint>> dpTest)
        {
            this.nodeTests = nodeTests;
            this.dpTest = dpTest;
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
            Logger.LogInfo($"Got {count} datapoints");
            dpTest?.Invoke(dataPointList);
        }
        public async Task PushDataPoints(CancellationToken token)
        {
            await Task.Run(() => SyncPushDps(BufferedDPQueue));
        }
        private void SyncPushNodes(ConcurrentQueue<BufferedNode> nodeQueue, CancellationToken token)
        {
            var nodeMap = new Dictionary<string, BufferedNode>();
            var assetList = new List<BufferedNode>();
            var varList = new List<BufferedVariable>();
            var histTsList = new List<BufferedVariable>();
            var tsList = new List<BufferedVariable>();

            int count = 0;
            while (nodeQueue.TryDequeue(out BufferedNode buffer))
            {
                if (buffer.IsVariable)
                {
                    var buffVar = (BufferedVariable)buffer;

                    if (buffVar.IsProperty)
                    {
                        nodeMap.TryGetValue(UAClient.GetUniqueId(buffVar.ParentId), out BufferedNode parent);
                        if (parent == null) continue;
                        if (parent.properties == null)
                        {
                            parent.properties = new List<BufferedVariable>();
                        }
                        parent.properties.Add(buffVar);
                    }
                    else
                    {
                        count++;
                        varList.Add(buffVar);
                    }
                }
                else
                {
                    count++;
                    assetList.Add(buffer);
                }
                nodeMap.TryAdd(UAClient.GetUniqueId(buffer.Id), buffer);
            }
            if (count == 0) return;
            UAClient.ReadNodeData(assetList.Concat(varList), token);
            foreach (var node in varList)
            {
                if (node.IsProperty) continue;
                if (Extractor.AllowTSMap(node))
                {
                    if (node.Historizing)
                    {
                        histTsList.Add(node);
                        lock (Extractor.NotInSyncLock)
                        {
                            Extractor.NotInSync.Add(UAClient.GetUniqueId(node.Id));
                        }
                    }
                    else
                    {
                        tsList.Add(node);
                    }
                }
            }
            nodeTests?.GetValueOrDefault("afterdata")?.Invoke(assetList, tsList, histTsList);
            UAClient.GetNodeProperties(assetList.Concat(tsList).Concat(histTsList), token);
            nodeTests?.GetValueOrDefault("afterProperties")?.Invoke(assetList, tsList, histTsList);
            Extractor.SynchronizeNodes(tsList.Concat(histTsList), token);
            nodeTests?.GetValueOrDefault("afterSynchronize")?.Invoke(assetList, tsList, histTsList);
            EndCB?.Invoke();
        }
        public async Task<bool> PushNodes(CancellationToken token)
        {
            await Task.Run(() => SyncPushNodes(BufferedNodeQueue, token));
            return true;
        }

        public void Reset()
        {
        }
    }
}
