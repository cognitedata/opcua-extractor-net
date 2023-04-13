using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Config
{
    public partial class UAServerExplorer : UAClient
    {
        /// <summary>
        /// Look for emitter relationships, and attempt to listen to events on any identified emitters. Also look through the event hierarchy and find any
        /// custom events that may be interesting for cognite.
        /// Enables events if it seems like the server supports them.
        /// </summary>
        public async Task GetEventConfig(CancellationToken token)
        {
            await PopulateNodes(token);
            await ReadNodeData(token);

            log.LogInformation("Test for event configuration");
            eventTypes.Clear();

            try
            {
                Config.Events.AllEvents = true;
                Config.Events.Enabled = true;
                await TypeManager.LoadTypeData(token);
                TypeManager.BuildTypeInfo();
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to read event types, the extractor will not be able to support events");
                return;
            }

            var server = await GetServerNode(token);

            var emitters = nodeList.Append(server).OfType<UAObject>().Where(node => (node.FullAttributes.EventNotifier & EventNotifiers.SubscribeToEvents) != 0);
            var historizingEmitters = emitters.Where(node => (node.FullAttributes.EventNotifier & EventNotifiers.HistoryRead) != 0);

            if (emitters.Any())
            {
                log.LogInformation("Discovered {EmitterCount} emitters, of which {HistCount} are historizing", emitters.Count(), historizingEmitters.Count());
                Summary.Events.NumEmitters = emitters.Count();
                Summary.Events.NumHistEmitters = historizingEmitters.Count();
                Summary.Events.AnyEvents = true;
                baseConfig.Events.Enabled = true;
                if (historizingEmitters.Any())
                {
                    Summary.Events.HistoricalEvents = true;
                    baseConfig.History.Enabled = true;
                    baseConfig.Events.History = true;
                }
            }

            log.LogInformation("Scan hierarchy for GeneratesEvent references");

            var emitterReferences = new List<BaseUANode>();
            try
            {
                await Browser.BrowseDirectory(nodeList.Select(node => node.Id).Append(ObjectIds.Server).ToList(),
                    ToolUtil.GetSimpleListWriterCallback(emitterReferences, this, TypeManager, log),
                    token,
                    ReferenceTypeIds.GeneratesEvent, (uint)NodeClass.ObjectType, false, purpose: "identifying GeneratesEvent references");
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Failed to look for GeneratesEvent references, this tool will not be able to identify emitted event types this way");
            }

            var referencedEvents = emitterReferences.Select(evt => evt.Id)
                .Distinct().ToHashSet();

            var emittedEvents = referencedEvents.ToList();

            if (emittedEvents.Any())
            {
                log.LogInformation("Identified {Count} events by looking at GeneratesEvent references", emittedEvents.Count);
                bool auditReferences = emitterReferences.Any(evt => evt.ParentId == ObjectIds.Server
                && (evt.Id == ObjectTypeIds.AuditAddNodesEventType || evt.Id == ObjectTypeIds.AuditAddReferencesEventType));

                Summary.Events.AnyEvents = true;

                baseConfig.Extraction.EnableAuditDiscovery |= auditReferences;
                Summary.Events.Auditing = auditReferences;

                if (auditReferences)
                {
                    log.LogInformation("Audit events on the server node detected, auditing can be enabled");
                }
            }

            if (!Summary.Events.Auditing)
            {
                try
                {
                    var results = await Session!.ReadAsync(
                        null,
                        0,
                        TimestampsToReturn.Neither,
                        new ReadValueIdCollection(new[] { new ReadValueId { NodeId = VariableIds.Server_Auditing, AttributeId = Attributes.Value } }),
                        token);
                    var result = results.Results.First().GetValue<bool>(false);
                    if (result)
                    {
                        log.LogInformation("Server capabilities indicate that auditing is enabled");
                        Summary.Events.Auditing = true;
                        baseConfig.Extraction.EnableAuditDiscovery = true;
                    }
                }
                catch (Exception ex)
                {
                    log.LogWarning(ex, "Failed to read auditing server configuration");
                }
            }

            if (!emitters.Any() || !historizingEmitters.Any())
            {
                log.LogInformation("No event configuration found");
                return;
            }

            log.LogInformation("Try subscribing to events on emitting nodes");

            var states = emitters.Select(emitter => new EventExtractionState(this, emitter.Id, false, false, true));

            try
            {
                await ToolUtil.RunWithTimeout(SubscribeToEvents(states.Take(baseConfig.Source.SubscriptionChunk), (item, args) => { }, TypeManager.EventFields, token), 120);
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Failed to subscribe to events. The extractor will not be able to support events.");
                return;
            }

            await Session!.RemoveSubscriptionsAsync(Session.Subscriptions.ToList());
        }
    }
}
