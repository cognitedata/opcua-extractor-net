using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Opc.Ua;
using Xunit;

namespace Test
{
    public class TestPusher : IPusher
    {
        public NodeId RootNode { get; set; }
        public Extractor Extractor { private get; set; }
        public UAClient UAClient { private get; set; }

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
                Assert.True(buffer.timestamp > 0L, "Invalid timestamp");
                dataPointList.Add(buffer);
            }
            Logger.LogInfo($"Got {count} datapoints");
            dpTest?.Invoke(dataPointList);
        }
        public async Task PushDataPoints(ConcurrentQueue<BufferedDataPoint> dataPointQueue)
        {
            await Task.Run(() => SyncPushDps(dataPointQueue));
        }
        private void SyncPushNodes(ConcurrentQueue<BufferedNode> nodeQueue)
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
                nodeMap.Add(UAClient.GetUniqueId(buffer.Id), buffer);
            }
            if (count == 0) return;
            UAClient.ReadNodeData(assetList.Concat(varList));
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
            UAClient.GetNodeProperties(assetList.Concat(tsList).Concat(histTsList));
            nodeTests?.GetValueOrDefault("afterProperties")?.Invoke(assetList, tsList, histTsList);
            Extractor.SynchronizeNodes(tsList.Concat(histTsList));
            nodeTests?.GetValueOrDefault("afterSynchronize")?.Invoke(assetList, tsList, histTsList);
        }
        public async Task<bool> PushNodes(ConcurrentQueue<BufferedNode> nodeQueue)
        {
            await Task.Run(() => SyncPushNodes(nodeQueue));
            return true;
        }

        public void Reset()
        {
        }
    }
}
