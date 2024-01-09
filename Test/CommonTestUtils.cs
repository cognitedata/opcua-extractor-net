/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Prometheus;
using Server;
using Xunit;

[assembly: CLSCompliant(false)]
namespace Test
{
    public static class CommonTestUtils
    {
        private static readonly object portCounterLock = new object();
        private static int portCounter = 62200;
        public static int NextPort
        {
            get
            {
                lock (portCounterLock)
                {
                    return portCounter++;
                }
            }
        }

        public static bool TestRunResult(Exception e)
        {
            if (!(e is TaskCanceledException || e is AggregateException && e.InnerException is TaskCanceledException))
            {
                return false;
            }
            return true;
        }
        public static void AddDummyProvider(string project, CDFMockHandler.MockMode mode, bool storeDatapoints, IServiceCollection services)
        {
            services.AddSingleton(provider =>
            {
                return new CDFMockHandler(project, mode, provider.GetRequiredService<ILogger<CDFMockHandler>>())
                {
                    StoreDatapoints = storeDatapoints
                };
            });
            services.AddHttpClient<Client.Builder>()
                .ConfigurePrimaryHttpMessageHandler(provider => provider.GetRequiredService<CDFMockHandler>().CreateHandler());
        }

        private static Collector GetCollector(string name)
        {
            var prop = Metrics.DefaultRegistry.GetType().GetField("_families", BindingFlags.NonPublic | BindingFlags.Instance);
            var dict = (IDictionary)prop.GetValue(Metrics.DefaultRegistry);
            object family;
            try
            {
                family = /* CollectorFamily */ dict[name];
            }
            catch
            {
                return null;
            }

            var collectors = /* ConcurrentDictionary<CollectorIdentity, Collector> */ family?.GetType().GetField("_collectors", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(family);
            if (collectors == null) return null;
            return ((IEnumerable<Collector>)collectors.GetType().GetProperty("Values").GetValue(collectors)).FirstOrDefault();
        }

        public static double GetMetricValue(string name)
        {
            var collector = GetCollector(name);
            return collector switch
            {
                Gauge gauge => gauge.Value,
                Counter counter => counter.Value,
                _ => 0
            };
        }
        public static bool TestMetricValue(string name, double value, ILogger log = null)
        {
            var collector = GetCollector(name);
            double val = collector switch
            {
                Gauge gauge => gauge.Value,
                Counter counter => counter.Value,
                _ => 0
            };
            if (Math.Abs(val - value) > 0.01)
            {
                log?.LogWarning("Expected {Value} but got {Val} for metric {Name}", value, val, name);
                return false;
            }

            return true;
        }


        public static void ResetMetricValues(params string[] names)
        {
            foreach (var name in names)
            {
                ResetMetricValue(name);
            }
        }
        public static void ResetMetricValue(string name)
        {
            var collector = GetCollector(name);
            switch (collector)
            {
                case Gauge gauge:
                    gauge.Set(0);
                    break;
                case Counter counter:
                    // See the prometheus-net source code. Since they refuse to make it possible to do anything
                    // not in the basic use case, this crazy dynamic hacking is necessary.
                    // This is not the best way to do things, and it might
                    // randomly break due to internal changes in prometheus-net.
                    // It does get the job done, however.

                    // Get the internal counter child (Counter.Child)
                    var internalChild = counter
                        .GetType()
                        .GetProperty("Unlabelled", BindingFlags.NonPublic | BindingFlags.Instance)
                        .GetValue(counter);
                    // Get the internal _value. The exposed Value property is read-only
                    var internalValue = internalChild
                        .GetType()
                        .GetField("_value", BindingFlags.NonPublic | BindingFlags.Instance)
                        .GetValue(internalChild);
                    // _value is a ThreadSafeDouble internal struct, so it cannot be modified easily
                    // for some reason modifying structs using reflection tends to just give you a new instance.
                    // We can, however, just create a new one.
                    var newSafeDouble = Activator.CreateInstance(internalValue.GetType(), 0.0);
                    internalChild.GetType()
                        .GetField("_value", BindingFlags.NonPublic | BindingFlags.Instance)
                        .SetValue(internalChild, newSafeDouble);
                    break;
            }
        }
        public static void ResetTestMetrics()
        {
            var metrics = new List<string>
            {
                "opcua_attribute_request_failures",
                "opcua_history_read_failures",
                "opcua_browse_failures",
                "opcua_browse_operations",
                "opcua_history_reads",
                "opcua_tracked_timeseries",
                "opcua_tracked_assets",
                "opcua_node_ensure_failures_cdf",
                "opcua_datapoint_pushes_cdf",
                "opcua_datapoint_push_failures_cdf",
                "opcua_frontfill_events",
                "opcua_backfill_events",
                "opcua_frontfill_data",
                "opcua_backfill_data",
                "opcua_backfill_data_count",
                "opcua_frontfill_data_count",
                "opcua_backfill_events_count",
                "opcua_frontfill_events_count",
                "opcua_datapoint_push_failures_influx",
                "opcua_event_push_failures_cdf",
                "opcua_event_push_failures_influx",
                "opcua_duplicated_events_cdf",
                "opcua_created_assets_mqtt",
                "opcua_created_timeseries_mqtt",
                "opcua_array_points_missed"
            };
            foreach (var metric in metrics)
            {
                ResetMetricValue(metric);
            }
        }

        private static Dictionary<string, string> MetaToDict(JsonElement elem)
        {
            if (elem.ValueKind != JsonValueKind.Object) return null;
            return elem.EnumerateObject().ToDictionary(kvp => kvp.Name, kvp => kvp.Value.ToString());
        }
        public static void VerifyStartingConditions(
            Dictionary<string, AssetDummy> assets,
            Dictionary<string, TimeseriesDummy> timeseries,
            UpdateConfig upd,
            IUAClientAccess client,
            CustomNodeReference ids,
            bool raw)
        {
            ArgumentNullException.ThrowIfNull(assets);
            ArgumentNullException.ThrowIfNull(timeseries);
            if (upd == null) upd = new UpdateConfig();
            ArgumentNullException.ThrowIfNull(client);
            ArgumentNullException.ThrowIfNull(ids);
            Assert.Equal(6, assets.Count);
            Assert.Equal(16, timeseries.Count);

            var rootId = client.GetUniqueId(ids.Root);
            var obj1Id = client.GetUniqueId(ids.Obj1);
            var obj2Id = client.GetUniqueId(ids.Obj2);
            var stringyId = client.GetUniqueId(ids.StringyVar);
            var mysteryId = client.GetUniqueId(ids.MysteryVar);

            if (!upd.Objects.Name) Assert.Equal("CustomRoot", assets[rootId].name);
            if (!upd.Objects.Description) Assert.True(string.IsNullOrEmpty(assets[rootId].description));


            if (!upd.Variables.Name) Assert.Equal("StringyVar", timeseries[stringyId].name);
            if (!upd.Variables.Description) Assert.True(string.IsNullOrEmpty(timeseries[stringyId].description));

            if (raw)
            {
                if (!upd.Variables.Context) Assert.Equal(rootId, (timeseries[stringyId] as StatelessTimeseriesDummy).assetExternalId);
            }
            else
            {
                if (!upd.Variables.Context) Assert.Equal(assets[rootId].id, timeseries[stringyId].assetId);
            }

            if (!upd.Objects.Context) Assert.Equal(rootId, assets[obj2Id].parentExternalId);

            Dictionary<string, string> obj1Meta, obj2Meta, stringyMeta, mysteryMeta;

            if (raw)
            {
                obj1Meta = MetaToDict((assets[obj1Id] as AssetDummyJson).metadata);
                obj2Meta = MetaToDict((assets[obj2Id] as AssetDummyJson).metadata);
                stringyMeta = MetaToDict((timeseries[stringyId] as StatelessTimeseriesDummy).metadata);
                mysteryMeta = MetaToDict((timeseries[mysteryId] as StatelessTimeseriesDummy).metadata);
            }
            else
            {
                obj1Meta = assets[obj1Id].metadata;
                obj2Meta = assets[obj2Id].metadata;
                stringyMeta = timeseries[stringyId].metadata;
                mysteryMeta = timeseries[mysteryId].metadata;
            }

            if (!upd.Objects.Metadata)
            {
                Assert.True(obj1Meta == null || obj1Meta.Count == 0);
                Assert.Equal(2, obj2Meta.Count);
                Assert.Equal("1234", obj2Meta["NumericProp"]);
            }
            if (!upd.Variables.Metadata)
            {
                Assert.True(stringyMeta == null || stringyMeta.Count == 0);
                Assert.Equal(2, mysteryMeta.Count);
                Assert.Equal("(0, 100)", mysteryMeta["EURange"]);
            }

        }

        public static void VerifyModified(
            Dictionary<string, AssetDummy> assets,
            Dictionary<string, TimeseriesDummy> timeseries,
            UpdateConfig upd,
            IUAClientAccess client,
            CustomNodeReference ids,
            bool raw)
        {
            ArgumentNullException.ThrowIfNull(assets);
            ArgumentNullException.ThrowIfNull(timeseries);
            if (upd == null) upd = new UpdateConfig();
            ArgumentNullException.ThrowIfNull(client);
            ArgumentNullException.ThrowIfNull(ids);
            Assert.Equal(6, assets.Count);
            Assert.Equal(16, timeseries.Count);

            var rootId = client.GetUniqueId(ids.Root);
            var obj1Id = client.GetUniqueId(ids.Obj1);
            var obj2Id = client.GetUniqueId(ids.Obj2);
            var stringyId = client.GetUniqueId(ids.StringyVar);
            var mysteryId = client.GetUniqueId(ids.MysteryVar);

            if (upd.Objects.Name) Assert.Equal("CustomRoot updated", assets[rootId].name);
            if (upd.Objects.Description) Assert.Equal("custom root description", assets[rootId].description);

            if (upd.Variables.Name) Assert.Equal("StringyVar updated", timeseries[stringyId].name);
            if (upd.Variables.Description) Assert.Equal("Stringy var description", timeseries[stringyId].description);
            if (raw)
            {
                if (upd.Objects.Context) Assert.Equal(obj1Id, assets[obj2Id].parentExternalId);
                if (upd.Variables.Context) Assert.Equal(obj1Id, (timeseries[stringyId] as StatelessTimeseriesDummy).assetExternalId);
            }
            else
            {
                if (upd.Objects.Context) Assert.Equal(obj1Id, assets[obj2Id].parentExternalId);
                if (upd.Variables.Context) Assert.Equal(assets[obj1Id].id, timeseries[stringyId].assetId);
            }

            Dictionary<string, string> obj1Meta, obj2Meta, stringyMeta, mysteryMeta;

            if (raw)
            {
                obj1Meta = MetaToDict((assets[obj1Id] as AssetDummyJson).metadata);
                obj2Meta = MetaToDict((assets[obj2Id] as AssetDummyJson).metadata);
                stringyMeta = MetaToDict((timeseries[stringyId] as StatelessTimeseriesDummy).metadata);
                mysteryMeta = MetaToDict((timeseries[mysteryId] as StatelessTimeseriesDummy).metadata);
            }
            else
            {
                obj1Meta = assets[obj1Id].metadata;
                obj2Meta = assets[obj2Id].metadata;
                stringyMeta = timeseries[stringyId].metadata;
                mysteryMeta = timeseries[mysteryId].metadata;
            }


            if (upd.Objects.Metadata)
            {
                Assert.Single(obj1Meta);
                Assert.Equal("New asset prop value", obj1Meta["NewAssetProp"]);
                Assert.Equal(2, obj2Meta.Count);
                Assert.Equal("4321", obj2Meta["NumericProp"]);
                Assert.True(obj2Meta.ContainsKey("StringProp updated"));
            }
            if (upd.Variables.Metadata)
            {
                Assert.Single(stringyMeta);
                Assert.Equal("New prop value", stringyMeta["NewProp"]);
                Assert.Equal(2, mysteryMeta.Count);
                Assert.Equal("(0, 200)", mysteryMeta["EURange"]);
            }
        }

        private static readonly JsonSerializerOptions jsonOptions = new JsonSerializerOptions
        {
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        public static string JsonElementToString(JsonElement elem)
        {
            return System.Text.Json.JsonSerializer.Serialize(elem, jsonOptions);
        }

        public static ProtoNodeId ToProtoNodeId(this NodeId id, UAClient client)
        {
            if (id == null || id.IsNullNodeId || client == null) return null;
            var buffer = new StringBuilder();
            NodeId.Format(buffer, id.Identifier, id.IdType, 0);
            var ns = client.NamespaceTable.GetString(id.NamespaceIndex);
            return new ProtoNodeId
            {
                NodeId = buffer.ToString(),
                NamespaceUri = ns
            };
        }
        public static UAVariable GetSimpleVariable(string name, UADataType dt, int dim = 0, NodeId id = null)
        {
            var variable = new UAVariable(id ?? new NodeId(name, 0), name, null, null, NodeId.Null, null);
            variable.FullAttributes.DataType = dt;
            variable.FullAttributes.ValueRank = ValueRanks.Scalar;
            if (dim > 0)
            {
                variable.FullAttributes.ArrayDimensions = new[] { dim };
                variable.FullAttributes.ValueRank = ValueRanks.OneDimension;
            }
            return variable;
        }

        public static async Task RunHistory(HistoryReader reader, IEnumerable<UAHistoryExtractionState> states, HistoryReadType type)
        {
            var task = (Task)reader.GetType().GetMethod("RunHistoryBatch", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(reader, new object[] { states, type });
            await task;
        }
    }
}
