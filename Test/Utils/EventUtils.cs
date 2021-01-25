using Cognite.OpcUa;
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using System;
using System.Linq;
using Test.Unit;

namespace Test.Utils
{
    public static class EventUtils
    {
        private static string[] baseFields = new[]
{
            "EventId", "SourceNode", "EventType", "Message", "Time"
        };

        public static EventExtractionState PopulateEventData(UAExtractor extractor, BaseExtractorTestFixture tester, bool init)
        {
            if (tester == null) throw new ArgumentNullException(nameof(tester));
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            // Add state
            var state = new EventExtractionState(tester.Client, new NodeId("emitter"), true, true);
            if (init)
            {
                state.InitExtractedRange(DateTime.UtcNow.Subtract(TimeSpan.FromHours(1)), DateTime.UtcNow.AddHours(1));
                state.FinalizeRangeInit();
            }
            extractor.State.SetEmitterState(state);

            var fields = baseFields.Select(field => new EventField(ObjectTypeIds.BaseEventType, new QualifiedName(field)));
            fields = fields.Append(new EventField(tester.Server.Ids.Custom.EUProp, new QualifiedName("EUProp")));
            extractor.State.ActiveEvents[new NodeId("test")] = fields.ToHashSet();

            return state;
        }

        public static SimpleAttributeOperandCollection GetSelectClause(BaseExtractorTestFixture tester)
        {
            if (tester == null) throw new ArgumentNullException(nameof(tester));
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
                new NodeId("source"),
                new NodeId("test"),
                new LocalizedText("message"),
                time,
                new int[] { 1, 2, 3 },
                new ExtensionObject(new EUInformation("unit", "uuuniiit", "uri"))
            };
        }
    }
}
