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

using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Serilog;
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
        protected virtual ILogger Log { get; set; } = Serilog.Log.Logger.ForContext(typeof(BaseNodeSource));
        // Initial collection of nodes, in a map.
        protected Dictionary<NodeId, UANode> NodeMap { get; } = new Dictionary<NodeId, UANode>();
        protected List<UANode> RawObjects { get; } = new List<UANode>();
        protected List<UAVariable> RawVariables { get; } = new List<UAVariable>();

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
        /// <summary>
        /// Apply transformations and sort the given node as variable, object or property.
        /// </summary>
        /// <param name="node"></param>
        protected void SortNode(UANode node)
        {
            bool initialProperty = node.IsProperty;
            if (node.ParentId != null && !node.ParentId.IsNullNodeId && NodeMap.TryGetValue(node.ParentId, out var parent))
            {
                node.Parent = parent;
            }

            if (Extractor.Transformations != null)
            {
                if (node.Parent != null)
                {
                    node.Attributes.Ignore |= node.Parent.Ignore;
                    node.Attributes.IsProperty |= node.Parent.IsProperty || node.Parent.NodeClass == NodeClass.Variable;
                    if (node.Parent.NodeClass == NodeClass.Variable || node.Parent.IsProperty)
                    {
                        node.Attributes.IsProperty = true;
                    }
                }
                foreach (var trns in Extractor.Transformations)
                {
                    trns.ApplyTransformation(node, Client.NamespaceTable!);
                    if (node.Ignore) return;
                }
            }

            if (node.IsProperty)
            {
                if (node.Parent == null) return;
                node.Parent.AddProperty(node);
                // Edge-case, since attributes are read before transformations, if transformations cause a node to become a property,
                // ArrayDimensions won't be read. We can just read them later at minimal cost.
                if (!initialProperty && Config.Extraction.DataTypes.MaxArraySize == 0 && (node is UAVariable variable) && variable.ValueRank >= 0)
                {
                    node.Attributes.DataRead = false;
                }
            }
            else if (node is UAVariable variable)
            {
                RawVariables.Add(variable);
            }
            else
            {
                RawObjects.Add(node);
            }
        }

        /// <summary>
        /// Filter a node, creating new objects and variables based on attributes and config.
        /// </summary>
        /// <param name="update">Configuration used to determine what nodes have changed.</param>
        /// <param name="node">Node to sort.</param>
        protected void SortVariable(UpdateConfig update, UAVariable node)
        {
            if (!Extractor.DataTypeManager.AllowTSMap(node)) return;
            if (update.AnyUpdate)
            {
                var oldChecksum = Extractor.State.GetNodeChecksum(node.Id);
                if (oldChecksum != null)
                {
                    node.Changed |= oldChecksum != node.GetUpdateChecksum(
                        update.Variables,
                        Config.Extraction.DataTypes.DataTypeMetadata,
                        Config.Extraction.NodeTypes.Metadata);

                    if (node.Changed)
                    {
                        AddVariableToLists(node);
                    }
                    return;
                }
            }

            Log.Verbose(node.ToString());
            AddVariableToLists(node);
        }

        /// <summary>
        /// Returns true if the raw object should be added to the final list of objects.
        /// </summary>
        /// <param name="update">Update configuration used to determine what nodes are changed.</param>
        /// <param name="node">Node to be filtered.</param>
        /// <returns>True if node should be considered for mapping, false otherwise.</returns>
        protected bool FilterObject(UpdateConfig update, UANode node)
        {
            if (update.AnyUpdate)
            {
                var oldChecksum = Extractor.State.GetNodeChecksum(node.Id);
                if (oldChecksum != null)
                {
                    node.Changed |= oldChecksum != node.GetUpdateChecksum(
                        update.Objects,
                        Config.Extraction.DataTypes.DataTypeMetadata,
                        Config.Extraction.NodeTypes.Metadata);
                    return node.Changed;
                }
            }
            Log.Verbose(node.ToString());

            return true;
        }
    }
}
