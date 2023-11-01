/* Cognite Extractor for OPC-UA
Copyright (C) 2023 Cognite AS

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

using Cognite.Extractor.Common;
using Cognite.OpcUa.NodeSources;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;
using YamlDotNet.Serialization;

namespace Cognite.OpcUa.Config
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1721:Property names should not match get methods", Justification = "Config")]
    public class ExtractionConfig
    {
        /// <summary>
        /// Global prefix for externalId towards pushers. Should be unique to prevent name conflicts in the push destinations.
        /// The generated externalId is: IdPrefix + NamespaceMap[nodeId.NamespaceUri] + nodeId.Identifier
        /// </summary>
        public string? IdPrefix { get; set; }
        /// <summary>
        /// DEPRECATED. Specify a list of prefixes on DisplayName to ignore.
        /// </summary>
        public IEnumerable<string>? IgnoreNamePrefix { get; set; }
        /// <summary>
        /// DEPRECATED. Specify a list of DisplayNames to ignore.
        /// </summary>
        public IEnumerable<string>? IgnoreName { get; set; }
        /// <summary>
        /// Root node. Defaults to the Objects node.
        /// </summary>
        public ProtoNodeId? RootNode { get; set; }
        /// <summary>
        /// List of proto-node-ids similar to root-node.
        /// The extractor will start exploring from these.
        /// Specifying nodes connected with hierarchical references can result in some strange behavior:
        /// generally, the node deeper in the hierarchy will be detached from its parent and excluded from the hierarchy of the other node.
        /// </summary>
        public IEnumerable<ProtoNodeId>? RootNodes { get; set; }
        /// <summary>
        /// Override mappings between OPC UA node id and externalId, allowing e.g. the RootNode to be mapped to
        /// a particular asset in CDF. Applies to both assets and time series.
        /// </summary>
        public Dictionary<string, ProtoNodeId>? NodeMap { get; set; }
        /// <summary>
        /// Map OPC-UA namespaces to prefixes in CDF. If not mapped, the full namespace URI is used.
        /// Saves space compared to using the full URL. Using the namespace index is not safe as the order can change on the server.
        /// </summary>
        public Dictionary<string, string> NamespaceMap { get => namespaceMap; set => namespaceMap = value ?? namespaceMap; }
        private Dictionary<string, string> namespaceMap = new Dictionary<string, string>();

        public CronTimeSpanWrapper AutoRebrowsePeriodValue { get; } = new CronTimeSpanWrapper(false, false, "m", "0");

        /// <summary>
        /// Time in minutes between each call to browse the OPC-UA directory, then push new nodes to destinations.
        /// Note that this is a heavy operation, so this number should not be set too low.
        /// Alternatively, use N[timeunit] where timeunit is w, d, h, m, s or ms.
        /// You can also use a cron expression on the form "[minute] [hour] [day of month] [month] [day of week]"
        /// </summary>
        public string? AutoRebrowsePeriod
        {
            get => AutoRebrowsePeriodValue.RawValue; set => AutoRebrowsePeriodValue.RawValue = value!;
        }
        /// Enable using audit events to discover new nodes. If this is set to true, the client will expect AuditAddNodes/AuditAddReferences
        /// events on the server node. These will be used to add new nodes automatically, by recursively browsing from each given ParentId.
        public bool EnableAuditDiscovery { get; set; }
        public TimeSpanWrapper DataPushDelayValue { get; } = new TimeSpanWrapper(true, "ms", "1000");
        /// <summary>
        /// Delay in ms between each push of data points to targets
        /// Alternatively, use N[timeunit] where timeunit is w, d, h, m, s or ms.
        /// </summary>
        public string? DataPushDelay
        {
            get => DataPushDelayValue.RawValue; set => DataPushDelayValue.RawValue = value!;
        }
        /// <summary>
        /// Update data in destinations on rebrowse or restart.
        /// Set auto-rebrowse-period to some value to do this periodically.
        /// Context refers to the structure of the node graph in OPC-UA. (assetId and parentId in CDF)
        /// Metadata refers to any information obtained from OPC-UA properties. (metadata in CDF)
        /// Enabling anything here will increase the startup- and rebrowse-time of the extractor.
        /// </summary>
        public UpdateConfig Update { get => update; set => update = value ?? update; }
        private UpdateConfig update = new UpdateConfig();
        /// <summary>
        /// Configuration for handling of data types in OPC-UA.
        /// </summary>
        public DataTypeConfig DataTypes { get => dataTypes; set => dataTypes = value ?? dataTypes; }
        private DataTypeConfig dataTypes = new DataTypeConfig();
        /// <summary>
        /// DEPRECATED. Regex filter on DisplayName to treat variables as properties.
        /// </summary>
        public string? PropertyNameFilter { get; set; }
        /// <summary>
        /// DEPRECATED. Regex filter on id to treat variables as properties.
        /// </summary>
        public string? PropertyIdFilter { get; set; }
        /// <summary>
        /// Configuration for translating OPC-UA references to relationships in CDF.
        /// </summary>
        public RelationshipConfig Relationships { get => relationships; set => relationships = value ?? relationships; }
        private RelationshipConfig relationships = new RelationshipConfig();
        /// <summary>
        /// Configuration for reading OPC-UA node types.
        /// </summary>
        public NodeTypeConfig NodeTypes { get => nodeTypes; set => nodeTypes = value ?? nodeTypes; }
        private NodeTypeConfig nodeTypes = new NodeTypeConfig();
        /// <summary>
        /// If true the extractor will try reading children of variables and map those to timeseries as well.
        /// </summary>
        public bool MapVariableChildren { get; set; }
        /// <summary>
        /// Configuration for enabling soft deletes.
        /// </summary>
        public DeletesConfig Deletes { get => deletes; set => deletes = value ?? deletes; }
        private DeletesConfig deletes = new DeletesConfig();

        /// <summary>
        /// A list of transformations to be applied to the source nodes before pushing
        /// The possible transformations are
        /// "Ignore", ignore the node. This will ignore all descendants of the node.
        /// If the filter does not use "is-array", "description" or "parent", this is done
        /// while reading, and so children will not be read at all. Otherwise, the filtering happens later.
        /// "Property", turn the node into a property, which is treated as metadata.
        /// This also applies to descendants. Nested metadata is give a name like "grandparent_parent_variable", for
        /// each variable in the tree.
        /// "DropSubscriptions", do not subscribe to this node with neither events or data-points.
        /// "TimeSeries", do not treat this variable as a property.
        /// There is some overhead associated with the filters. They are applied sequentially, so it can help performance to put
        /// "Ignore" filters first. This is also worth noting when it comes to TimeSeries transformations, which can undo Property
        /// transformations.
        /// It is possible to have multiple of each filter type.
        /// </summary>
        public IEnumerable<RawNodeTransformation>? Transformations { get; set; }
        public IEnumerable<NodeId> GetRootNodes(SessionContext context)
        {
            var roots = new List<NodeId>();
            var protoRoots = RootNodes ?? Enumerable.Empty<ProtoNodeId>();
            if (RootNode != null && RootNode.NamespaceUri != null && RootNode.NodeId != null)
            {
                protoRoots = protoRoots.Prepend(RootNode);
            }
            foreach (var root in protoRoots)
            {
                var id = root.ToNodeId(context);
                if (id.IsNullNodeId)
                {
                    throw new ConfigurationException($"Failed to convert configured root node {root.NamespaceUri} {root.NodeId} to NodeId");
                }
                roots.Add(id);
            }
            if (!roots.Any())
            {
                roots.Add(ObjectIds.ObjectsFolder);
            }
            return roots.Distinct().ToArray();
        }
        /// <summary>
        /// Server namespace nodes the should be subscribed to a rebrowse upon changes to their values.
        /// </summary>
        public RebrowseTriggersConfig? RebrowseTriggers { get; set; }
    }
    public class DataTypeConfig
    {
        /// <summary>
        /// Add custom numeric types using their nodeId. is-step indicates whether the datatype is discrete,
        /// enum indicates that it is an enumeration, which may be mapped to a string if enums-as-strings is true.
        /// This also overwrite default behavior, so it is possible to make Integer discrete, etc.
        /// Note that the type in question needs to have a sensible numerical conversion in C#, unless it is an array type or similar, 
        /// in which case each element needs a conversion
        /// </summary>
        public IEnumerable<ProtoDataType>? CustomNumericTypes { get; set; }
        /// <summary>
        /// List of NodeIds corresponding to DataTypes that should be ignored. Timeseries with these datatypes will not be mapped to destinations.
        /// </summary>
        public IEnumerable<ProtoNodeId>? IgnoreDataTypes { get; set; }
        /// <summary>
        /// Assume unknown ValueRanks without ArrayDimensions are all scalar, and create timeseries in CDF accordingly.
        /// If such a variable produces an array, only the first element will be mapped to CDF
        /// </summary>
        public bool UnknownAsScalar { get; set; }
        /// <summary>
        /// Maximum size of array variables. Only arrays with the ArrayDimensions property in opc-ua specified will be used,
        /// leave at 0 to only allow scalar values.
        /// Note that some server implementations have issues with the ArrayDimensions property, so it is not fetched at all if MaxArraySize is 0
        /// -1 indicates that there is no limit to array length, though only 1-dimensional structures will be read either way.
        /// </summary>
        public int MaxArraySize { get; set; }
        /// <summary>
        /// Set to true to allow fetching string variables. This means that all variables with non-numeric type is converted to string in some way.
        /// </summary>
        public bool AllowStringVariables { get; set; }
        /// <summary>
        /// Map out the dataType hierarchy before starting, useful if there are custom or enum types.
        /// Necessary for enum metadata and for enums-as-strings to work. If this is false, any
        /// custom numeric types have to be added manually.
        /// </summary>
        public bool AutoIdentifyTypes { get; set; }
        /// <summary>
        /// If this is false and auto-identify-types is true, or there are manually added enums in custom-numeric-types,
        /// enums will be mapped to numeric timeseries, and labels are added as metadata fields.
        /// If this is true, labels are not mapped to metadata, and enums will be mapped to string timeseries with values
        /// equal to mapped label values.
        /// </summary>
        public bool EnumsAsStrings { get; set; }
        /// <summary>
        /// Add a metadata property dataType which contains the id of the OPC-UA datatype.
        /// </summary>
        public bool DataTypeMetadata { get; set; }
        /// <summary>
        /// True to treat null nodeIds as numeric instead of string
        /// </summary>
        public bool NullAsNumeric { get; set; }
        /// <summary>
        /// Add full JSON node-ids to data pushed to Raw. TypeDefintionId, ParentNodeId, NodeId and DataTypeId.
        /// </summary>
        public bool ExpandNodeIds { get; set; }
        /// <summary>
        /// Add attributes generally used internally like AccessLevel, Historizing, ArrayDimensions, ValueRank etc.
        /// to data pushed to Raw.
        /// </summary>
        public bool AppendInternalValues { get; set; }
        /// <summary>
        /// If max-array-size is set, this looks for the MaxArraySize property on each node with one-dimension ValueRank,
        /// if it is not found, it tries to read the value as well, and look at the current size.
        /// ArrayDimensions is still the prefered way to identify array sizes, this is not guaranteed to generate
        /// reasonable or useful values.
        /// </summary>
        public bool EstimateArraySizes { get; set; }

        /// <summary>
        /// If true, variables not mapped due to array dimensions or data type are all mapped to properties instead.
        /// </summary>
        public bool UnmappedAsProperties { get; set; }
    }

    public class RelationshipConfig
    {
        /// <summary>
        /// True to enable mapping OPC-UA references to relationships in CDF.
        /// </summary>
        public bool Enabled { get; set; }
        /// <summary>
        /// True to enable also mapping the hierarchical references over. These are the ones that are normally
        /// mapped to assetId/parentId relations in CDF. In that case the type is lost.
        /// Requires relationships.enabled to be true.
        /// </summary>
        public bool Hierarchical { get; set; }
        /// <summary>
        /// True to create inverse relationships for each of the hierarchical references.
        /// For efficiency these are not read, but inferred from forwards references,
        /// they will almost always be there in practice.
        /// Does nothing if hierarchical is false.
        /// </summary>
        public bool InverseHierarchical { get; set; }
        /// <summary>
        /// Create any nodes that are found through non-hierarchical references but not in the hierarchy.
        /// </summary>
        public bool CreateReferencedNodes { get; set; }

        [YamlIgnore]
        public HierarchicalReferenceMode Mode
        {
            get
            {
                if (!Enabled || !Hierarchical) return HierarchicalReferenceMode.Disabled;
                if (!InverseHierarchical) return HierarchicalReferenceMode.Forward;
                return HierarchicalReferenceMode.Both;
            }
        }
    }
    public class NodeTypeConfig
    {
        /// <summary>
        /// Add the TypeDefinition as a metadata field to all nodes.
        /// </summary>
        public bool Metadata { get; set; }
        /// <summary>
        /// Allow reading object- and vairable types as normal nodes and map them to destinations.
        /// They will need to be in the mapped hierarchy.
        /// To actually get types in the node hierarchy you have to add a root node that they descend from.
        /// </summary>
        public bool AsNodes { get; set; }
    }

    public class UpdateConfig
    {
        public bool AnyUpdate => objects.AnyUpdate || variables.AnyUpdate;
        /// <summary>
        /// Configuration for updating objects and object types.
        /// </summary>
        public TypeUpdateConfig Objects { get => objects; set => objects = value ?? objects; }
        private TypeUpdateConfig objects = new TypeUpdateConfig();
        /// <summary>
        /// Configuration for updating variables and variable types.
        /// </summary>
        public TypeUpdateConfig Variables { get => variables; set => variables = value ?? variables; }
        private TypeUpdateConfig variables = new TypeUpdateConfig();
    }
    public class TypeUpdateConfig
    {
        public bool AnyUpdate => Description || Name || Metadata || Context;
        /// <summary>
        /// True to update description.
        /// </summary>
        public bool Description { get; set; }
        /// <summary>
        /// True to update name.
        /// </summary>
        public bool Name { get; set; }
        /// <summary>
        /// True to update metadata.
        /// </summary>
        public bool Metadata { get; set; }
        /// <summary>
        /// True to update context, i.e. the position of the node in the node hierarchy.
        /// </summary>
        public bool Context { get; set; }
    }

    public class DeletesConfig
    {
        /// <summary>
        /// Enable deletes.
        /// </summary>
        public bool Enabled { get; set; }

        /// <summary>
        /// Name of marker for indicating that a node is deleted.
        /// Added to metadata, as a column on raw rows, or similar.
        /// </summary>
        public string DeleteMarker { get; set; } = "deleted";
    }

    public class RebrowseTriggersConfig
    {
        public RebrowseTriggerTargets Targets { get => targets; set { targets = value ?? targets; } }

        private RebrowseTriggerTargets targets = new RebrowseTriggerTargets();

        /// <summary>
        /// A list of namespaces filters
        /// </summary>
        public IEnumerable<string> Namespaces { get; set; } = new List<string>();
    }

    public class RebrowseTriggerTargets
    {
        private List<string> ToBeSubscribed = new List<string>();

        public bool NamespacePublicationDate
        {
            get => ToBeSubscribed.Contains("NamespacePublicationDate");
            set
            {
                var exists = ToBeSubscribed.Contains("NamespacePublicationDate");

                if (value && !exists)
                {
                    ToBeSubscribed.Add("NamespacePublicationDate");
                }
                else if (!value && exists)
                {
                    ToBeSubscribed.Remove("NamespacePublicationDate");
                }
            }
        }

        public List<string> GetTargets => ToBeSubscribed;
    }
    public class RawNodeFilter
    {
        /// <summary>
        /// Regex on node DisplayName.
        /// </summary>
        public string? Name { get; set; }
        /// <summary>
        /// Regex on node Description.
        /// </summary>
        public string? Description { get; set; }
        /// <summary>
        /// Regex on node id. Ids on the form "i=123" or "s=string" are matched.
        /// </summary>
        public string? Id { get; set; }
        /// <summary>
        /// Whether the node is an array. If this is set, the filter only matches varables.
        /// </summary>
        public bool? IsArray { get; set; }
        /// <summary>
        /// Regex on the full namespace of the node id.
        /// </summary>
        public string? Namespace { get; set; }
        /// <summary>
        /// Regex on the id of the type definition. On the form "i=123" or "s=string".
        /// </summary>
        public string? TypeDefinition { get; set; }
        /// <summary>
        /// The "historizing" attribute on variables. If this is set, the filter only matches variables.
        /// </summary>
        public bool? Historizing { get; set; }
        /// <summary>
        /// The OPC-UA node class, exact match. Should be one of
        /// "Object", "ObjectType", "Variable", "VariableType".
        /// </summary>
        public NodeClass? NodeClass { get; set; }
        /// <summary>
        /// Another instance of NodeFilter which is applied to the parent node.
        /// </summary>
        public RawNodeFilter? Parent { get; set; }
    }
    public class RawNodeTransformation
    {
        /// <summary>
        /// Type, either "Ignore", "Property", "DropSubscriptions" or "TimeSeries"
        /// </summary>
        public TransformationType Type { get; set; }
        /// <summary>
        /// NodeFilter. All non-null filters must match each node for the transformation to be applied.
        /// </summary>
        public RawNodeFilter? Filter { get; set; }
    }
}
