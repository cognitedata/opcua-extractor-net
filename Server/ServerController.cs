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

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Server
{
    /// <summary>
    /// Convenient wrapper for the server object handling starting/stopping it, 
    /// and various utility methods used for testing.
    /// </summary>
    sealed public class ServerController : IDisposable
    {
        public NodeIdReference Ids => Server.Ids;
        private readonly ILogger log;
        public TestServer Server { get; private set; }
        private readonly IEnumerable<PredefinedSetup> setups;
        private readonly int port;
        public ServerIssueConfig Issues => Server.Issues;
        public string ConfigRoot { get; set; } = "Server.Test";
        private readonly string mqttUrl;
        private readonly string endpointUrl;
        private readonly bool logTrace;
        private readonly IServiceProvider provider;
        private bool running;
        private IEnumerable<string> nodeSetFiles;

        public ServerController(
            IEnumerable<PredefinedSetup> setups,
            IServiceProvider provider,
            int port = 62546,
            string mqttUrl = "mqtt://localhost:4060",
            string endpointUrl = "opc.tcp://localhost",
            bool logTrace = false,
            IEnumerable<string> nodeSetFiles = null)
        {
            this.setups = setups;
            this.port = port;
            this.mqttUrl = mqttUrl;
            this.endpointUrl = endpointUrl;
            this.logTrace = logTrace;
            log = provider.GetRequiredService<ILogger<ServerController>>();
            this.provider = provider;
            this.nodeSetFiles = nodeSetFiles;
        }

        public void Dispose()
        {
            log.LogInformation("Closing server");
            Server?.Stop();
            Server?.Dispose();
        }

        public async Task Start()
        {
            if (running) return;
            var app = new ApplicationInstance
            {
                ConfigSectionName = ConfigRoot
            };
            try
            {
                var cfg = await app.LoadApplicationConfiguration(Path.Join("config", $"{ConfigRoot}.Config.xml"), false);
                var address = cfg.ServerConfiguration.BaseAddresses[0] = $"{endpointUrl}:{port}";
                await app.CheckApplicationInstanceCertificate(false, 0);
                Server = new TestServer(setups, mqttUrl, provider, logTrace, nodeSetFiles);
                await Task.Run(async () => await app.Start(Server));
                log.LogInformation("Server started on address: {Address}", address);
                running = true;
            }
            catch (Exception e)
            {
                log.LogError(e, "Failed to start server");
                throw;
            }
        }
        public void Stop()
        {
            log.LogInformation("Closing server");
            Server.Stop();
            running = false;
        }

        public static Func<int, StatusCode> GetStatusGenerator()
        {
            var random = new Random();
            StatusCode code = StatusCodes.Good;
            return idx =>
            {
                if ((idx % 10) == 0)
                {
#pragma warning disable CA5394 // Do not use insecure randomness
                    code = new StatusCode(RandomCodes[random.Next(0, RandomCodes.Length)]);
#pragma warning restore CA5394 // Do not use insecure randomness
                }
                return code;
            };
        }

        public void PopulateCustomHistory(DateTime? start = null, bool randomStatusCodes = false)
        {
            Func<int, StatusCode> statusBuilder = randomStatusCodes ? GetStatusGenerator() : null;

            if (start == null)
            {
                start = DateTime.UtcNow.AddMilliseconds(-1000 * 10);
            }
            Server.PopulateHistory(Server.Ids.Custom.Array, 1000, start.Value, "custom", 10, i => new int[] { i, i, i, i }, statusBuilder: statusBuilder);
            Server.PopulateHistory(Server.Ids.Custom.MysteryVar, 1000, start.Value, "int", statusBuilder: statusBuilder);
            Server.PopulateHistory(Server.Ids.Custom.StringyVar, 1000, start.Value, "string", statusBuilder: statusBuilder);
        }

        public static readonly uint[] RandomCodes = new[] {
            StatusCodes.Good,
            StatusCodes.Bad,
            StatusCodes.Uncertain,
            StatusCodes.GoodClamped,
            StatusCodes.BadOutOfRange
        };

        public void PopulateBaseHistory(DateTime? start = null, bool randomStatusCodes = false, bool notifyLast = true)
        {
            Func<int, StatusCode> statusBuilder = randomStatusCodes ? GetStatusGenerator() : null;

            if (start == null)
            {
                start = DateTime.UtcNow.AddMilliseconds(-1000 * 10);
            }
            Server.PopulateHistory(Server.Ids.Base.DoubleVar1, 1000, start.Value, "double", statusBuilder: statusBuilder, notifyLast: notifyLast);
            Server.PopulateHistory(Server.Ids.Base.StringVar, 1000, start.Value, "string", statusBuilder: statusBuilder, notifyLast: notifyLast);
            Server.PopulateHistory(Server.Ids.Base.IntVar, 1000, start.Value, "int", statusBuilder: statusBuilder, notifyLast: notifyLast);
        }

        public void UpdateNode(NodeId id, object value, StatusCode? code = null)
        {
            Server.UpdateNode(id, value, code);
        }

        public async Task UpdateNodeMultiple(NodeId id, int count, Func<int, object> generator, int delayms = 50)
        {
            ArgumentNullException.ThrowIfNull(generator);
            for (int i = 0; i < count; i++)
            {
                Server.UpdateNode(id, generator(i));
                await Task.Delay(delayms);
            }
        }

        public void TriggerEvents(int idx)
        {
            // Test emitters and properties
            Server.TriggerEvent<PropertyEvent>(Ids.Event.PropType, ObjectIds.Server, Ids.Event.Obj1, "prop " + idx, evt =>
            {
                var revt = evt;
                revt.PropertyString.Value = "str " + idx;
                revt.PropertyNum.Value = idx;
                revt.SubType.Value = "sub-type";
            });
            Server.TriggerEvent<PropertyEvent>(Ids.Event.PropType, Ids.Event.Obj1, Ids.Event.Obj1, "prop-e2 " + idx, evt =>
            {
                var revt = evt;
                revt.PropertyString.Value = "str o2 " + idx;
                revt.PropertyNum.Value = idx;
                revt.SubType.Value = "sub-type";
            });
            Server.TriggerEvent<PropertyEvent>(Ids.Event.PropType, Ids.Event.Obj2, Ids.Event.Obj1, "prop-e3 " + idx, evt =>
            {
                var revt = evt;
                revt.PropertyString.Value = "str o3 - " + idx;
                revt.PropertyNum.Value = idx;
                revt.SubType.Value = "sub-type";
            });
            // Test types
            Server.TriggerEvent<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Obj1, "basic-pass " + idx);
            Server.TriggerEvent<BasicEvent2>(Ids.Event.BasicType2, ObjectIds.Server, Ids.Event.Obj1, "basic-block " + idx);
            Server.TriggerEvent<CustomEvent>(Ids.Event.CustomType, ObjectIds.Server, Ids.Event.Obj1, "mapped " + idx, evt =>
            {
                var revt = evt;
                revt.TypeProp.Value = "CustomType";
            });

            // Test sources
            Server.TriggerEvent<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Obj2, "basic-pass-2 " + idx);
            Server.TriggerEvent<BasicEvent1>(Ids.Event.BasicType1, Ids.Event.Obj1, Ids.Event.Obj2, "basic-pass-3 " + idx);
            Server.TriggerEvent<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Var1, "basic-varsource " + idx);
            Server.TriggerEvent<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, null, "basic-nosource " + idx);
            Server.TriggerEvent<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.ObjExclude, "basic-excludeobj " + idx);
        }

        public void PopulateEvents(DateTime? start = null)
        {
            if (start == null)
            {
                start = DateTime.UtcNow.AddMilliseconds(-100 * 100);
            }
            Server.PopulateEventHistory<PropertyEvent>(Ids.Event.PropType, ObjectIds.Server, Ids.Event.Obj1, "prop", 100, start.Value, 100, (evt, idx) =>
            {
                var revt = evt as PropertyEvent;
                revt.PropertyString.Value = "str " + idx;
                revt.PropertyNum.Value = idx;
                revt.SubType.Value = "sub-type";
            });
            Server.PopulateEventHistory<PropertyEvent>(Ids.Event.PropType, Ids.Event.Obj1, Ids.Event.Obj1, "prop-e2", 100, start.Value, 100, (evt, idx) =>
            {
                var revt = evt as PropertyEvent;
                revt.PropertyString.Value = "str o2 " + idx;
                revt.PropertyNum.Value = idx;
                revt.SubType.Value = "sub-type";
            });
            // Test types
            Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Obj1, "basic-pass", 100, start.Value, 100);
            Server.PopulateEventHistory<BasicEvent2>(Ids.Event.BasicType2, ObjectIds.Server, Ids.Event.Obj1, "basic-block", 100, start.Value, 100);
            Server.PopulateEventHistory<CustomEvent>(Ids.Event.CustomType, ObjectIds.Server, Ids.Event.Obj1, "mapped", 100, start.Value, 100, (evt, idx) =>
            {
                var revt = evt as CustomEvent;
                revt.TypeProp.Value = "CustomType";
            });

            // Test sources
            Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Obj2, "basic-pass-2", 100, start.Value, 100);
            Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, Ids.Event.Obj1, Ids.Event.Obj2, "basic-pass-3", 100, start.Value, 100);
            Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Var1, "basic-varsource", 100, start.Value, 100);
            Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, null, "basic-nosource", 100, start.Value, 100);
            Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.ObjExclude, "basic-excludeobj", 100, start.Value, 100);
        }

        public void DirectGrowth(int idx = 0)
        {
            Server.AddObject(Ids.Audit.DirectAdd, "AddObj " + idx, true);
            Server.AddVariable(Ids.Audit.DirectAdd, "AddVar " + idx, DataTypes.Double, true);
        }
        public void ReferenceGrowth(int idx = 0)
        {
            var objId = Server.AddObject(Ids.Audit.ExcludeObj, "AddObj " + idx, true);
            var varId = Server.AddVariable(Ids.Audit.ExcludeObj, "AddVar " + idx, DataTypes.Double, true);
            Server.AddReference(objId, Ids.Audit.RefAdd, ReferenceTypeIds.HasComponent, true);
            Server.AddReference(varId, Ids.Audit.RefAdd, ReferenceTypeIds.HasComponent, true);
        }
        public void ModifyCustomServer()
        {
            Server.MutateNode(Ids.Custom.Root, root =>
            {
                root.Description = new LocalizedText("custom root description");
                root.DisplayName = new LocalizedText("CustomRoot updated");
            });
            Server.MutateNode(Ids.Custom.StringyVar, node =>
            {
                node.Description = new LocalizedText("Stringy var description");
                node.DisplayName = new LocalizedText("StringyVar updated");
            });
            Server.ReContextualize(Ids.Custom.Obj2, Ids.Custom.Root, Ids.Custom.Obj1, ReferenceTypeIds.Organizes);
            Server.ReContextualize(Ids.Custom.StringyVar, Ids.Custom.Root, Ids.Custom.Obj1, ReferenceTypeIds.HasComponent);

            Server.AddProperty<string>(Ids.Custom.StringyVar, "NewProp", DataTypeIds.String, "New prop value");
            Server.AddProperty<string>(Ids.Custom.Obj1, "NewAssetProp", DataTypeIds.String, "New asset prop value");

            Server.MutateNode(Ids.Custom.RangeProp, node =>
            {
                if (node is not PropertyState prop) return;
                prop.Value = new Opc.Ua.Range(200, 0);
            });
            Server.MutateNode(Ids.Custom.ObjProp, node =>
            {
                if (node is not PropertyState prop) return;
                prop.Value = 4321L;
            });
            Server.MutateNode(Ids.Custom.EUProp, node =>
            {
                if (node is not PropertyState prop) return;
                prop.DisplayName = new LocalizedText("EngineeringUnits updated");
            });
            Server.MutateNode(Ids.Custom.ObjProp2, node =>
            {
                if (node is not PropertyState prop) return;
                prop.DisplayName = new LocalizedText("StringProp updated");
            });
        }
        public void ResetCustomServer()
        {
            Server.MutateNode(Ids.Custom.Root, root =>
            {
                root.Description = null;
                root.DisplayName = new LocalizedText("CustomRoot");
            });
            Server.MutateNode(Ids.Custom.StringyVar, node =>
            {
                node.Description = null;
                node.DisplayName = new LocalizedText("StringyVar");
            });
            Server.ReContextualize(Ids.Custom.Obj2, Ids.Custom.Obj1, Ids.Custom.Root, ReferenceTypeIds.Organizes);
            Server.ReContextualize(Ids.Custom.StringyVar, Ids.Custom.Obj1, Ids.Custom.Root, ReferenceTypeIds.HasComponent);

            Server.RemoveProperty(Ids.Custom.StringyVar, "NewProp");
            Server.RemoveProperty(Ids.Custom.Obj1, "NewAssetProp");

            Server.MutateNode(Ids.Custom.RangeProp, node =>
            {
                if (node is not PropertyState prop) return;
                prop.Value = new Opc.Ua.Range(100, 0);
            });
            Server.MutateNode(Ids.Custom.ObjProp, node =>
            {
                if (node is not PropertyState prop) return;
                prop.Value = 1234L;
            });
            Server.MutateNode(Ids.Custom.EUProp, node =>
            {
                if (node is not PropertyState prop) return;
                prop.DisplayName = new LocalizedText("EngineeringUnits");
            });
            Server.MutateNode(Ids.Custom.ObjProp2, node =>
            {
                if (node is not PropertyState prop) return;
                prop.DisplayName = new LocalizedText("StringProp");
            });
        }
        public void WipeHistory(NodeId id, object value)
        {
            Server.WipeHistory(id, value);
        }
        public void WipeEventHistory(NodeId id = null)
        {
            Server.WipeEventHistory(id);
        }
        public void SetEventConfig(bool auditing, bool server, bool serverAuditing)
        {
            Server.SetEventConfig(auditing, server, serverAuditing);
        }
        public void SetDiagnosticsEnabled(bool value)
        {
            Server.SetDiagnosticsEnabled(value);
        }

        public void UpdateBaseNodes(int idx, StatusCode? code = null)
        {
            UpdateNode(Ids.Base.DoubleVar1, idx, code);
            UpdateNode(Ids.Base.DoubleVar2, -idx, code);
            UpdateNode(Ids.Base.BoolVar, idx % 2 == 0, code);
            UpdateNode(Ids.Base.IntVar, idx, code);
            UpdateNode(Ids.Base.StringVar, $"Idx: {idx}", code);
        }

        public void UpdateCustomNodes(int idx, StatusCode? code = null)
        {
            UpdateNode(Ids.Custom.Array, new double[] { idx, idx + 1, idx + 2, idx + 3 }, code);
            UpdateNode(Ids.Custom.StringArray, new string[] { $"str{idx}", $"str{-idx}" }, code);
            UpdateNode(Ids.Custom.StringyVar, $"Idx: {idx}", code);
            UpdateNode(Ids.Custom.MysteryVar, idx, code);
            UpdateNode(Ids.Custom.IgnoreVar, $"Idx: {idx}", code);
            UpdateNode(Ids.Custom.NumberVar, idx, code);
            UpdateNode(Ids.Custom.EnumVar1, idx % 3, code);
            UpdateNode(Ids.Custom.EnumVar2, idx % 2 == 0 ? 123 : 321, code);
            UpdateNode(Ids.Custom.EnumVar3, idx % 2 == 0
                ? new[] { 123, 123, 321, 123 } : new[] { 123, 123, 123, 321 }, code);
        }

        public void SetServerRedundancyStatus(byte serviceLevel, RedundancySupport support)
        {
            Server.SetServerRedundancyStatus(serviceLevel, support);
        }
    }
}
