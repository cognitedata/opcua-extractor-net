using Opc.Ua;
using Opc.Ua.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Server
{
    sealed public class ServerController : IDisposable
    {
        public NodeIdReference Ids => Server.Ids;
        public Server Server { get; private set; }
        private IEnumerable<PredefinedSetup> setups;

        public ServerController(IEnumerable<PredefinedSetup> setups)
        {
            this.setups = setups;
        }

        public void Dispose()
        {
            Log.Information("Closing server");
            Server?.Stop();
            Server?.Dispose();
        }

        public async Task Start()
        {
            ApplicationInstance app = new ApplicationInstance();
            app.ConfigSectionName = "Server.Test";
            try
            {
                app.LoadApplicationConfiguration("config/Server.Test.Config.xml", false).Wait();
                app.CheckApplicationInstanceCertificate(false, 0).Wait();
                Server = new Server(setups);
                await app.Start(Server);
                Log.Information("Server started");
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to start server");
            }
        }
        public void Stop()
        {
            Server.Stop();
        }

        public void PopulateArrayHistory()
        {
            Server.PopulateHistory(Server.Ids.Custom.Array, 1000, "custom", 10, (i => new int[] { i, i, i, i }));
            Server.PopulateHistory(Server.Ids.Custom.MysteryVar, 1000, "int");
        }
        public void PopulateBaseHistory()
        {
            Server.PopulateHistory(Server.Ids.Base.DoubleVar1, 1000, "double");
            Server.PopulateHistory(Server.Ids.Base.StringVar, 1000, "string");
            Server.PopulateHistory(Server.Ids.Base.IntVar, 1000, "int");
        }

        public void UpdateNode(NodeId id, object value)
        {
            Server.UpdateNode(id, value);
        }

        public async Task UpdateNodeMultiple(NodeId id, int count, Func<int, object> generator, int delayms = 50)
        {
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
                var revt = evt as PropertyEvent;
                revt.PropertyString.Value = "str " + idx;
                revt.PropertyNum.Value = idx;
                revt.SubType.Value = "sub-type";
            });
            Server.TriggerEvent<PropertyEvent>(Ids.Event.PropType, Ids.Event.Obj1, Ids.Event.Obj1, "prop-e2 " + idx, evt =>
            {
                var revt = evt as PropertyEvent;
                revt.PropertyString.Value = "str o2 " + idx;
                revt.PropertyNum.Value = idx;
                revt.SubType.Value = "sub-type";
            });
            Server.TriggerEvent<PropertyEvent>(Ids.Event.PropType, Ids.Event.Obj2, Ids.Event.Obj1, "prop-e3 " + idx, evt =>
            {
                var revt = evt as PropertyEvent;
                revt.PropertyString.Value = "str o3 - " + idx;
                revt.PropertyNum.Value = idx;
                revt.SubType.Value = "sub-type";
            });
            // Test types
            Server.TriggerEvent<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Obj1, "basic-pass " + idx);
            Server.TriggerEvent<BasicEvent2>(Ids.Event.BasicType2, ObjectIds.Server, Ids.Event.Obj1, "basic-block " + idx);
            Server.TriggerEvent<CustomEvent>(Ids.Event.CustomType, ObjectIds.Server, Ids.Event.Obj1, "mapped " + idx, evt =>
            {
                var revt = evt as CustomEvent;
                revt.TypeProp.Value = "CustomType";
            });

            // Test sources
            Server.TriggerEvent<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Obj2, "basic-pass-2 " + idx);
            Server.TriggerEvent<BasicEvent1>(Ids.Event.BasicType1, Ids.Event.Obj1, Ids.Event.Obj2, "basic-pass-3 " + idx);
            Server.TriggerEvent<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Var1, "basic-varsource " + idx);
            Server.TriggerEvent<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, null, "basic-nosource " + idx);
            Server.TriggerEvent<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.ObjExclude, "basic-excludeobj " + idx);
        }

        public void PopulateEvents()
        {
            Server.PopulateEventHistory<PropertyEvent>(Ids.Event.PropType, ObjectIds.Server, Ids.Event.Obj1, "prop", 100, 100, (evt, idx) =>
            {
                var revt = evt as PropertyEvent;
                revt.PropertyString.Value = "str " + idx;
                revt.PropertyNum.Value = idx;
                revt.SubType.Value = "sub-type";
            });
            Server.PopulateEventHistory<PropertyEvent>(Ids.Event.PropType, Ids.Event.Obj1, Ids.Event.Obj1, "prop-e2", 100, 100, (evt, idx) =>
            {
                var revt = evt as PropertyEvent;
                revt.PropertyString.Value = "str o2 " + idx;
                revt.PropertyNum.Value = idx;
                revt.SubType.Value = "sub-type";
            });
            // Test types
            Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Obj1, "basic-pass", 100, 100);
            Server.PopulateEventHistory<BasicEvent2>(Ids.Event.BasicType2, ObjectIds.Server, Ids.Event.Obj1, "basic-block", 100, 100);
            Server.PopulateEventHistory<CustomEvent>(Ids.Event.CustomType, ObjectIds.Server, Ids.Event.Obj1, "mapped", 100, 100, (evt, idx) =>
            {
                var revt = evt as CustomEvent;
                revt.TypeProp.Value = "CustomType";
            });

            // Test sources
            Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Obj2, "basic-pass-2", 100, 100);
            Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, Ids.Event.Obj1, Ids.Event.Obj2, "basic-pass-3", 100, 100);
            Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Var1, "basic-varsource", 100, 100);
            Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, null, "basic-nosource", 100, 100);
            Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.ObjExclude, "basic-excludeobj", 100, 100);
        }
    }
}
