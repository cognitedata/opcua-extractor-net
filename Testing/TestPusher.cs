﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Opc.Ua;
using Xunit;

namespace Testing
{
    public class TestPusher : IPusher
    {
        public NodeId RootNode { get; set; }
        public Extractor Extractor { private get; set; }
        public UAClient UAClient { private get; set; }

        public ISet<string> NotInSync { get; private set; } = new HashSet<string>();

        public object NotInSyncLock { get; private set; } = new object();
        int totalDps;
        private void SyncPushDps(ConcurrentQueue<BufferedDataPoint> dataPointQueue)
        {
            var dataPointList = new List<BufferedDataPoint>();
            int count = 0;
            while (dataPointQueue.TryDequeue(out BufferedDataPoint buffer) && count++ < 100000)
            {
                Assert.True(buffer.timestamp > 0L, "Invalid timestamp");
                dataPointList.Add(buffer);
            }
            Logger.LogInfo("Got " + count + " datapoints");
            totalDps += count;
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
                        lock (NotInSyncLock)
                        {
                            NotInSync.Add(UAClient.GetUniqueId(node.Id));
                        }
                    }
                    else
                    {
                        tsList.Add(node);
                    }
                }
            }
            Assert.Single(assetList);
            Assert.Single(tsList);
            Assert.Single(histTsList);
            UAClient.GetNodeProperties(assetList.Concat(tsList).Concat(histTsList));
            Assert.NotNull(histTsList.First().properties);
            Assert.Equal(2, histTsList.First().properties.Count);
            Assert.Equal(2, assetList.First().properties.Count);
            Extractor.SynchronizeNodes(tsList.Concat(histTsList));
            Thread.Sleep(2000);
            Assert.True(totalDps > 0, "Excepted some datapoints");
            int lastDps = totalDps;
            Thread.Sleep(2000);
            Assert.True(totalDps > lastDps, "Expected dps to be increasing");
            Environment.Exit(0);
        }
        public async Task PushNodes(ConcurrentQueue<BufferedNode> nodeQueue)
        {
            await Task.Run(() => SyncPushNodes(nodeQueue));
        }

        public void Reset()
        {
        }
    }
}
