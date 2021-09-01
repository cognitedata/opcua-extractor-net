using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    public enum NodeSource
    {
        CDF,
        OPCUA
    }

    /// <summary>
    /// Class containing some common behavior between node sources
    /// </summary>
    public abstract class BaseNodeSource
    {
        // Nodes that are treated as variables (and synchronized) in the source system
        protected List<UAVariable> FinalSourceVariables { get; } = new List<UAVariable>();
        // Nodes that are treated as objects (so not synchronized) in the source system.
        // finalSourceVariables and finalSourceObjects should together contain all mapped nodes
        // in the source system.
        protected List<UANode> FinalSourceObjects { get; } = new List<UANode>();

        // Nodes that are treated as objects in the destination systems (i.e. mapped to assets)
        protected List<UANode> FinalDestinationObjects { get; } = new List<UANode>();
        // Nodes that are treated as variables in the destination systems (i.e. mapped to timeseries)
        // May contain duplicate NodeIds, but all should produce distinct UniqueIds.
        protected List<UAVariable> FinalDestinationVariables { get; } = new List<UAVariable>();
        protected HashSet<UAReference> FinalReferences { get; } = new HashSet<UAReference>();

        protected FullConfig Config { get; }
        protected UAExtractor Extractor { get; }
        protected UAClient Client { get; }

        protected BaseNodeSource(FullConfig config, UAExtractor extractor, UAClient client)
        {
            Config = config;
            Extractor = extractor;
            Client = client;
        }


        public abstract Task<BrowseResult?> ParseResults(CancellationToken token);

        /// <summary>
        /// Write a variable to the correct output lists. This assumes the variable should be mapped.
        /// </summary>
        /// <param name="node">Variable to write</param>
        protected virtual void AddVariableToLists(UAVariable node)
        {
            if (node.IsArray)
            {
                FinalDestinationVariables.AddRange(node.CreateArrayChildren());
            }

            if (node.IsArray || node.NodeClass != NodeClass.Variable)
            {
                FinalDestinationObjects.Add(node);
            }
            else
            {
                FinalDestinationVariables.Add(node);
            }

            if (node.NodeClass == NodeClass.Variable)
            {
                FinalSourceVariables.Add(node);
            }
            else
            {
                FinalSourceObjects.Add(node);
            }
        }
        /// <summary>
        /// Write the node to the extractor state
        /// </summary>
        /// <param name="update">Update configuration</param>
        /// <param name="node">Node to store</param>
        protected virtual void InitNodeState(UpdateConfig update, UANode node)
        {
            var updateConfig = node is UAVariable ? update.Variables : update.Objects;

            Extractor.State.AddActiveNode(
                node,
                updateConfig,
                Config.Extraction.DataTypes.DataTypeMetadata,
                Config.Extraction.NodeTypes.Metadata);

            if (Config.Events.Enabled
                && node.EventNotifier != 0
                && (node.NodeClass == NodeClass.Variable || node.NodeClass == NodeClass.Object)
                && Extractor.State.GetEmitterState(node.Id) == null)
            {
                bool history = (node.EventNotifier & EventNotifiers.HistoryRead) != 0 && Config.Events.History;
                bool subscription = (node.EventNotifier & EventNotifiers.SubscribeToEvents) != 0 && node.ShouldSubscribe;
                var eventState = new EventExtractionState(Extractor, node.Id, history, history && Config.History.Backfill, subscription);
                Extractor.State.SetEmitterState(eventState);
            }

            if (node is UAVariable variable && variable.NodeClass == NodeClass.Variable)
            {
                var state = Extractor.State.GetNodeState(node.Id);
                if (state != null) return;

                bool setState = Config.Subscriptions.DataPoints || Config.History.Enabled && Config.History.Data;


                if (setState)
                {
                    state = new VariableExtractionState(
                        Extractor,
                        variable,
                        variable.ReadHistory,
                        variable.ReadHistory && Config.History.Backfill);
                }


                if (variable.IsArray)
                {
                    foreach (var child in variable.CreateArrayChildren())
                    {
                        var uniqueId = Extractor.GetUniqueId(child.Id, child.Index);
                        if (setState && state != null) Extractor.State.SetNodeState(state, uniqueId);
                        Extractor.State.RegisterNode(node.Id, uniqueId);
                    }
                }
                if (setState && state != null)
                {
                    Extractor.State.SetNodeState(state);
                }
            }
        }
    }
}
