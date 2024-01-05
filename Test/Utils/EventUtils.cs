using Cognite.OpcUa;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Opc.Ua;
using System;
using System.Linq;

namespace Test.Utils
{
    public static class EventUtils
    {
        private static readonly string[] baseFields = new[]
{
            "EventId", "SourceNode", "EventType", "Message", "Time"
        };

        public static EventExtractionState PopulateEventData(UAExtractor extractor, BaseExtractorTestFixture tester, bool init)
        {
            ArgumentNullException.ThrowIfNull(tester);
            ArgumentNullException.ThrowIfNull(extractor);
            // Add state
            var state = new EventExtractionState(tester.Client, new NodeId("emitter", 0), true, true, true);
            if (init)
            {
                state.InitExtractedRange(DateTime.UtcNow.Subtract(TimeSpan.FromHours(1)), DateTime.UtcNow.AddHours(1));
                state.FinalizeRangeInit();
            }
            extractor.State.SetEmitterState(state);

            var fields = baseFields.Select(field => new TypeField(new UAVariable(new NodeId(field, 0), field, new QualifiedName(field), null, null, null)));
            fields = fields.Append(new TypeField(new UAVariable(new NodeId("EUProp", 0), "EUProp", new QualifiedName("EUProp"), null, null, null)));
            var type = new UAObjectType(new NodeId("test", 0), "TestEvent", null, null, null);
            type.AllCollectedFields = fields.ToHashSet();
            extractor.State.ActiveEvents[new NodeId("test", 0)] = type;

            return state;
        }

        public static SimpleAttributeOperandCollection GetSelectClause(BaseExtractorTestFixture tester)
        {
            ArgumentNullException.ThrowIfNull(tester);
            var attrs = baseFields.Select(field => new SimpleAttributeOperand(ObjectTypeIds.BaseEventType, new QualifiedName(field)));
            attrs = attrs.Append(new SimpleAttributeOperand(tester.Server.Ids.Custom.Array, new QualifiedName("Array"))); // some other field
            attrs = attrs.Append(new SimpleAttributeOperand(tester.Server.Ids.Custom.EUProp, new QualifiedName("EUProp")));

            return new SimpleAttributeOperandCollection(attrs);
        }

        public static VariantCollection GetEventValues(DateTime time)
        {
            return new Variant[]
            {
                new byte[] { 0, 0, 0, 0, 2 },
                new NodeId("source", 0),
                new NodeId("test", 0),
                new LocalizedText("message"),
                time,
                new int[] { 1, 2, 3 },
                new ExtensionObject(new EUInformation("unit", "uuuniiit", "uri"))
            };
        }
    }
}
