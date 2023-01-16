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

using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Runtime.Serialization;
using System.Text.RegularExpressions;
using System.Threading;
using YamlDotNet.Serialization;

namespace Cognite.OpcUa
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1056:Uri properties should not be strings", Justification = "Yaml Deserialization")]
    public class UAClientConfig
    {
        /// <summary>
        /// Path to config files folder.
        /// </summary>
        [DefaultValue("config")]
        public string ConfigRoot { get; set; } = "config";
        /// <summary>
        /// URL of OPC-UA server endpoint or discovery server endpoint.
        /// </summary>
        [Required]
        public string? EndpointUrl { get; set; }
        /// <summary>
        /// Alternate endpoint URLs, used for redundancy if the server supports it.
        /// </summary>
        public IEnumerable<string>? AltEndpointUrls { get; set; }
        /// <summary>
        /// True to auto accept untrusted certificates.
        /// If this is false, server certificates must be trusted by manually moving them to the "trusted" certificates folder.
        /// </summary>
        [DefaultValue(true)]
        public bool AutoAccept { get; set; } = true;
        /// <summary>
        /// Interval between OPC-UA subscription publish requests.
        /// This is how frequently the extractor requests updates from the server.
        /// </summary>
        [DefaultValue(500)]
        public int PublishingInterval { get; set; } = 500;
        /// <summary>
        /// Requested sample interval per variable on the server.
        /// This is how often the extractor requests the server sample changes to values.
        /// The server has no obligation to use this value, or to use sampling at all,
        /// but on compliant servers this sets the lowest rate of changes.
        /// </summary>
        [DefaultValue(100)]
        public int SamplingInterval { get; set; } = 100;
        /// <summary>
        /// Requested length of queue for each variable on the server.
        /// </summary>
        [DefaultValue(100)]
        public int QueueLength { get; set; } = 100;
        /// <summary>
        /// OPC-UA username, can be left out to use anonymous authentication.
        /// </summary>
        public string? Username { get; set; }
        /// <summary>
        /// OPC-UA password, can be left out to use anonymous authentication.
        /// </summary>
        public string? Password { get; set; }
        /// <summary>
        /// True if the extractor should try to connect to a secure endpoint on the server.
        /// </summary>
        public bool Secure { get; set; }
        /// <summary>
        /// True if the extractor should exit and fully restart if the server goes down.
        /// If core server attributes change on restart leaving this as false may cause issues.
        /// Some servers handle reconnect poorly. In those cases this may have to be set to true.
        /// </summary>
        public bool ForceRestart { get; set; }
        /// <summary>
        /// True if the extractor should quit completely when it fails to start, instead of restarting.
        /// Combined with force-restart, this will cause the extractor to exit completely on disconnect.
        /// </summary>
        public bool ExitOnFailure { get; set; }
        /// <summary>
        /// Number of nodes per browse request. Large numbers are likely to exceed the servers tolerance.
        /// Lower numbers increase startup time.
        /// </summary>
        [DefaultValue(1000)]
        [Range(1, 100_000)]
        public int BrowseNodesChunk { get => browseNodesChunk; set => browseNodesChunk = Math.Max(1, value); }
        private int browseNodesChunk = 1000;
        /// <summary>
        /// Number of maximum requested results per node during browse. The server may return fewer.
        /// Setting this lower increases startup times. Setting it to 0 leaves the decision entirely up to the server.
        /// </summary>
        [DefaultValue(1000)]
        [Range(0, 100_000)]
        public int BrowseChunk { get => browseChunk; set => browseChunk = Math.Max(0, value); }
        private int browseChunk = 1000;
        /// <summary>
        /// Number of attributes per request. The extractor will read 5-10 attributes per node,
        /// so setting this too low will massively increase startup times.
        /// Setting it too high will likely exceed server limits.
        /// </summary>
        [DefaultValue(10_000)]
        [Range(1, 1_000_000)]
        public int AttributesChunk { get => attributesChunk; set => attributesChunk = Math.Max(1, value); }
        private int attributesChunk = 10000;
        /// <summary>
        /// Number of monitored items to create per request. Setting this lower will increase startup times.
        /// High values may exceed server limits.
        /// </summary>
        [DefaultValue(1000)]
        [Range(1, 100_000)]
        public int SubscriptionChunk { get => subscriptionChunk; set => subscriptionChunk = Math.Max(1, value); }
        private int subscriptionChunk = 1000;
        /// <summary>
        /// Time between each keep-alive request to the server. The third failed keep-alive request will time out the extractor
        /// and trigger a reconnect or restart.
        /// Setting this lower will trigger reconnect logic faster.
        /// Setting this too high makes it meaningless.
        /// </summary>
        [DefaultValue(5000)]
        [Range(100, 60_000)]
        public int KeepAliveInterval { get; set; } = 5000;
        /// <summary>
        /// Restart the extractor on reconnect, browsing the node hierarchy and recreating subscriptions.
        /// This may be necessary if the server is expected to change after reconnecting, but it may be
        /// too expensive if connection is often lost to the server.
        /// </summary>
        public bool RestartOnReconnect { get; set; }
        /// <summary>
        /// Configure settings for using an x509-certificate as login credentials.
        /// This is separate from the application certificate, and used for servers with
        /// automatic systems for authentication.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "x509-certificate")]
        [DataMember(Name = "x509-certificate")]
        public X509CertConfig? X509Certificate { get; set; }
        /// <summary>
        /// Local URL used for reverse-connect. This is the URL the server should connect to. An endpoint-url should also be specified,
        /// so that the extractor knows where it should accept requests from.
        /// The server is responsible for initiating connections, meaning it can be placed entirely behind a firewall.
        /// </summary>
        public string? ReverseConnectUrl { get; set; }
        /// <summary>
        /// Ignore all issues caused by the server certificate. This potentially opens the extractor up to a man in the middle attack,
        /// but can be used if the server is noncompliant and on a closed network.
        /// </summary>
        public bool IgnoreCertificateIssues { get; set; }
        /// <summary>
        /// Settings for throttling browse opeartions.
        /// </summary>
        public ContinuationPointThrottlingConfig BrowseThrottling
        {
            get => browseThrottling; set => browseThrottling = value ?? browseThrottling;
        }
        private ContinuationPointThrottlingConfig browseThrottling = new ContinuationPointThrottlingConfig();
        /// <summary>
        /// Configuration for using NodeSet2 files as sources for the OPC-UA node hierarchy instead of browsing the server.
        /// This can be used if the server structure is well known and fixed. Values of nodes are still read from the server.
        /// </summary>
        public NodeSetSourceConfig? NodeSetSource { get; set; }
        /// <summary>
        /// Limit chunking values based on exposed information from the server, if any.
        /// This can be set to false if the server info is known to be wrong, but should generally
        /// be left on, as exceeding configured server limits will almost certainly cause a crash.
        /// </summary>
        [DefaultValue(true)]
        public bool LimitToServerConfig { get; set; } = true;
        /// <summary>
        /// If an alternative source for the node hierarchy is used, like CDF or nodeset files, this can be set to true
        /// to also browse the node hierarchy in the background. This is useful to start the extractor quickly but
        /// also discover everything in the server.
        /// </summary>
        public bool AltSourceBackgroundBrowse { get; set; }
        /// <summary>
        /// Default application certificate expiry in months.
        /// The certificate may also be replaced manually by modifying the .xml config file.
        /// </summary>
        [DefaultValue(60)]
        [Range(1, 65535)]
        public ushort CertificateExpiry { get; set; } = 60;
        /// <summary>
        /// Configuration for retrying operations against the OPC-UA server.
        /// </summary>
        public UARetryConfig Retries { get => retries; set => retries = value ?? retries; }
        private UARetryConfig retries = new UARetryConfig();
    }

    public class UARetryConfig : RetryUtilConfig
    {
        /// <summary>
        /// List of numeric status codes to retry, in addition to a set of default codes.
        /// </summary>
        public IEnumerable<uint>? RetryStatusCodes { get => retryStatusCodes; set
        {
            retryStatusCodes = value;
            finalRetryStatusCodes = new HashSet<uint>((retryStatusCodes ?? Enumerable.Empty<uint>()).Concat(internalRetryStatusCodes));
        } }
        private IEnumerable<uint>? retryStatusCodes;


        private readonly IEnumerable<uint> internalRetryStatusCodes = new[]
        {
            StatusCodes.Bad,
            StatusCodes.BadConnectionClosed,
            StatusCodes.BadConnectionRejected,
            StatusCodes.BadNotConnected,
            StatusCodes.BadServerHalted,
            StatusCodes.BadServerNotConnected,
            StatusCodes.BadTimeout,
            StatusCodes.BadSecureChannelClosed,
            StatusCodes.BadNoCommunication
        };

        private HashSet<uint>? finalRetryStatusCodes;

        [YamlIgnore]
        public HashSet<uint> FinalRetryStatusCodes { get
        {
            if (finalRetryStatusCodes == null)
            {
                finalRetryStatusCodes = new HashSet<uint>((retryStatusCodes ?? Enumerable.Empty<uint>()).Concat(internalRetryStatusCodes));
                if (RetryStatusCodes != null)
                {
                    foreach (var code in RetryStatusCodes) finalRetryStatusCodes.Add(code);
                }
            }
            return finalRetryStatusCodes;
        } }
    }

    public enum X509CertificateLocation
    {
        None,
        User,
        Local
    };
    public class X509CertConfig
    {
        /// <summary>
        /// Path to local x509-certificate.
        /// </summary>
        public string? FileName { get; set; }
        /// <summary>
        /// Password to local x509-certificate file.
        /// </summary>
        public string? Password { get; set; }
        /// <summary>
        /// Local certificate store to use, one of None (to use file), Local (for LocalMachine) or User
        /// </summary>
        [DefaultValue(X509CertificateLocation.None)]
        public X509CertificateLocation Store { get; set; } = X509CertificateLocation.None;
        /// <summary>
        /// Name of certificate in store, e.g. CN=my-certificate
        /// </summary>
        public string? CertName { get; set; }
    }
    public class NodeSetConfig
    {
        /// <summary>
        /// Name of nodset file.
        /// </summary>
        public string? FileName { get; set; }
        /// <summary>
        /// Url of publicly available nodeset file.
        /// </summary>
        public Uri? Url { get; set; }
    }
    public class NodeSetSourceConfig
    {
        /// <summary>
        /// List of nodesets to read. Specified by URL, file name, or both. If no name is specified, the last segment of
        /// the URL is used as file name.
        /// File name is path both of downloaded files, and where the extractor looks for existing files.
        /// </summary>
        public IEnumerable<NodeSetConfig>? NodeSets { get; set; }
        /// <summary>
        /// Use nodeset files to replace the OPC-UA instance hierarchy, i.e. everything under "Objects" in the OPC-UA server.
        /// </summary>
        public bool Instance { get; set; }
        /// <summary>
        /// Use nodeset files to replace the OPC-UA type hierarchy.
        /// </summary>
        public bool Types { get; set; }
    }
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
        public IEnumerable<NodeId> GetRootNodes(UAClient client)
        {
            var roots = new List<NodeId>();
            if (RootNode != null)
            {
                roots.Add(RootNode.ToNodeId(client, ObjectIds.ObjectsFolder));
            }
            if (RootNodes != null)
            {
                roots.AddRange(RootNodes.Select(proto =>
                    proto.ToNodeId(client, ObjectIds.ObjectsFolder)));
            }
            if (!roots.Any())
            {
                roots.Add(ObjectIds.ObjectsFolder);
            }
            return roots.Distinct().ToArray();
        }
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
    public class DataSubscriptionConfig
    {
        /// <summary>
        /// What changes to a variable trigger an update.
        /// One of Status, StatusValue, or StatusValueTimestamp.
        /// </summary>
        [DefaultValue(DataChangeTrigger.StatusValue)]
        public DataChangeTrigger Trigger { get => filter.Trigger; set => filter.Trigger = value; }
        /// <summary>
        /// Deadband for numeric values.
        /// One of None, Absolute, or Percent.
        /// </summary>
        [DefaultValue(DeadbandType.None)]
        public DeadbandType DeadbandType { get => (DeadbandType)filter.DeadbandType; set => filter.DeadbandType = (uint)value; }
        /// <summary>
        /// Value of deadband.
        /// </summary>
        public double DeadbandValue { get => filter.DeadbandValue; set => filter.DeadbandValue = value; }
        private readonly DataChangeFilter filter = new DataChangeFilter()
        {
            Trigger = DataChangeTrigger.StatusValue,
            DeadbandType = (uint)DeadbandType.None,
            DeadbandValue = 0.0
        };
        public DataChangeFilter Filter => filter;
    }
    public class SubscriptionConfig
    {
        /// <summary>
        /// Modify the DataChangeFilter used for datapoint subscriptions. See OPC-UA reference part 4 7.17.2 for details.
        /// These are just passed to the server, they have no effect on extractor behavior.
        /// Filters are applied to all nodes, but deadband should only affect some, according to the standard.
        /// </summary>
        public DataSubscriptionConfig? DataChangeFilter { get; set; }
        /// <summary>
        /// Enable subscriptions on data-points.
        /// </summary>
        public bool DataPoints { get; set; } = true;
        /// <summary>
        /// Enable subscriptions on events. Requires events.enabled to be set to true.
        /// </summary>
        public bool Events { get; set; } = true;
        /// <summary>
        /// Ignore the access level parameter for history and datapoints.
        /// This means using the "Historizing" parameter for history, and subscribing to all timeseries, independent of AccessLevel.
        /// </summary>
        public bool IgnoreAccessLevel { get; set; }
        /// <summary>
        /// Log bad subscription datapoints
        /// </summary>
        public bool LogBadValues { get; set; } = true;

        public ServerNamespacesToRebrowseConfig? ServerNamespacesToRebrowse { get; set; }
    }
    public class ServerNamespacesToRebrowseConfig
    {
        public bool Subscribe { get; set; } = false;

        public IEnumerable<ProtoNodeId> NamespaceNames { get; set; } = new List<ProtoNodeId>();
    }
    public interface IPusherConfig
    {
        /// <summary>
        /// True to not write to destination, as a kind of dry-run for this destination.
        /// </summary>
        bool Debug { get; set; }
        /// <summary>
        /// If applicable, read the ranges of extracted variables from the destination.
        /// </summary>
        bool ReadExtractedRanges { get; set; }
        /// <summary>
        /// Replacement for NaN values.
        /// </summary>
        public double? NonFiniteReplacement { get; set; }
    }
    public class CognitePusherConfig : CogniteConfig, IPusherConfig
    {
        /// <summary>
        /// Data set to use for new objects. Existing objects will not be updated.
        /// </summary>
        public long? DataSetId { get; set; }
        /// <summary>
        /// Data set to use for new objects, overridden by data-set-id. Requires the capability datasets:read for the given data set.
        /// </summary>
        public string? DataSetExternalId { get; set; }
        /// <summary>
        ///  Debug mode, if true, Extractor will not push to target
        /// </summary>
        public bool Debug { get; set; }
        /// <summary>
        /// Whether to read start/end-points on startup, where possible. At least one pusher should be able to do this,
        /// otherwise back/frontfill will run for the entire history every restart.
        /// The CDF pusher is not able to read start/end points for events, so if reading historical events is enabled, one other pusher
        /// able to do this should be enabled.
        /// The state-store can do all this, if the state-store is enabled this can still be enabled if timeseries have been deleted from CDF
        /// and need to be re-read from history.
        /// </summary>
        [DefaultValue(true)]
        public bool ReadExtractedRanges { get; set; } = true;
        /// <summary>
        /// Do not push any metadata at all. If this is true, plain timeseries without metadata will be created,
        /// similarly to raw-metadata, and datapoints will be pushed. Nothing will be written to raw, and no assets will be created.
        /// Events will be created, but without asset context.
        /// </summary>
        public bool SkipMetadata { get; set; }
        /// <summary>
        /// Store assets and/or timeseries data in raw. Assets will not be created at all,
        /// timeseries will be created with just externalId, isStep and isString.
        /// Both timeseries and assets will be persisted in their entirety to raw.
        /// Datapoints are not affected, events will be created, but without asset context. The externalId
        /// of the source node is added to metadata if applicable.
        /// Use different table names for assets and timeseries.
        /// </summary>
        public RawMetadataConfig? RawMetadata { get; set; }
        /// <summary>
        /// Map metadata to asset/timeseries attributes. Each of "assets" and "timeseries" is a map from property DisplayName to
        /// CDF attribute. Legal attributes are "name, description, parentId" and "unit" for timeseries. "parentId" must somehow refer to
        /// an existing asset. For timeseries it must be a mapped asset, for assets it can be any asset.
        /// Example usage:
        /// timeseries:
        ///    "EngineeringUnits": "unit"
        ///    "EURange": "description"
        /// assets:
        ///    "Name": "name"
        /// </summary>
        public MetadataMapConfig? MetadataMapping { get; set; }
        /// <summary>
        /// Read from CDF instead of OPC-UA when starting, to speed up start on slow servers.
        /// Requires extraction.data-types.expand-node-ids and append-internal-values to be set to true.
        ///
        /// This should generally be enabled along with skip-metadata or raw-metadata
        /// If browse-on-empty is set to true, and raw-metadata is configured with the same
        /// database and tables, the extractor will read into raw on first run, then use raw later,
        /// and the raw database can be deleted to reset on next read.
        /// </summary>
        public CDFNodeSourceConfig? RawNodeBuffer { get; set; }
        /// <summary>
        /// Replacement for NaN values.
        /// </summary>
        public double? NonFiniteReplacement
        {
            get => NanReplacement;
            set => NanReplacement = value == null || double.IsFinite(value.Value)
                && value.Value > CogniteUtils.NumericValueMin
                && value.Value < CogniteUtils.NumericValueMax ? value : null;
        }
        /// <summary>
        /// Specification for a CDF function that is called after nodes are pushed to CDF,
        /// reporting the number changed.
        /// </summary>
        public BrowseCallbackConfig? BrowseCallback { get; set; }
    }
    public class RawMetadataConfig
    {
        /// <summary>
        /// Database to store data in, required.
        /// </summary>
        [Required]
        public string? Database { get; set; }
        /// <summary>
        /// Table to store assets in.
        /// </summary>
        public string? AssetsTable { get; set; }
        /// <summary>
        /// Table to store timeseries in.
        /// </summary>
        public string? TimeseriesTable { get; set; }
        /// <summary>
        /// Table to store relationships in
        /// </summary>
        public string? RelationshipsTable { get; set; }
    }
    public class MetadataMapConfig
    {
        public Dictionary<string, string>? Assets { get; set; }
        public Dictionary<string, string>? Timeseries { get; set; }
    }
    public class CDFNodeSourceConfig
    {
        /// <summary>
        /// Enable the raw node buffer.
        /// </summary>
        public bool Enable { get; set; }
        /// <summary>
        /// Raw database to read from.
        /// </summary>
        public string? Database { get; set; }
        /// <summary>
        /// Table to read assets from, for events.
        /// </summary>
        public string? AssetsTable { get; set; }
        /// <summary>
        /// Table to read timeseries from.
        /// </summary>
        public string? TimeseriesTable { get; set; }
        /// <summary>
        /// Run normal browse if nothing is found when reading from CDF, either because the tables are empty, or they do not exist.
        /// No valid nodes must be found to run this at all, meaning it may run if there are nodes, but none of them are
        /// potentially valid extraction targets.
        /// </summary>
        public bool BrowseOnEmpty { get; set; }
    }
    public class BrowseCallbackConfig : FunctionCallConfig
    {
        /// <summary>
        /// Call callback even if zero items are created or updated.
        /// </summary>
        public bool ReportOnEmpty { get; set; }
    }
    public class InfluxPusherConfig : IPusherConfig
    {
        /// <summary>
        /// URL of the host influxdb server.
        /// </summary>
        [Required]
        public string? Host { get; set; }
        /// <summary>
        /// Username to use for the influxdb server.
        /// </summary>
        public string? Username { get; set; }
        /// <summary>
        /// Password to use for the influxdb server.
        /// </summary>
        public string? Password { get; set; }
        /// <summary>
        /// Database to connect to on the influxdb server.
        /// </summary>
        [Required]
        public string? Database { get; set; }
        /// <summary>
        /// Max number of points to send in each request to influx.
        /// </summary>
        [DefaultValue(100_000)]
        public int PointChunkSize { get; set; } = 100000;
        /// <summary>
        /// Debug mode, if true, Extractor will not push to target.
        /// </summary>
        public bool Debug { get; set; }
        /// <summary>
        /// Whether to read start/end-points on startup, where possible. At least one pusher should be able to do this,
        /// or the state store should be enabled,
        /// otherwise back/frontfill will run for the entire history every restart.
        /// </summary>
        public bool ReadExtractedRanges { get; set; } = true;
        /// <summary>
        /// Whether to read start/end-points for events on startup, where possible. At least one pusher should be able to do this,
        /// or the state store should be enabled,
        /// otherwise back/frontfill will run for the entire history every restart.
        /// </summary>
        public bool ReadExtractedEventRanges { get; set; } = true;
        /// <summary>
        /// Replace all instances of NaN or Infinity with this floating point number. If left empty, ignore instead.
        /// </summary>
        public double? NonFiniteReplacement
        {
            get => nonFiniteReplacement;
            set
            {
                if (value == null) return;
                nonFiniteReplacement = double.IsFinite(value.Value) ? value : null;
            }
        }
        private double? nonFiniteReplacement;
    }
    public class MqttPusherConfig : IPusherConfig
    {
        /// <summary>
        /// TCP Broker URL.
        /// </summary>
        public string? Host { get; set; }
        /// <summary>
        /// TCP Broker port.
        /// </summary>
        public int? Port { get; set; }
        /// <summary>
        /// MQTT broker username.
        /// </summary>
        public string? Username { get; set; }
        /// <summary>
        /// MQTT broker password.
        /// </summary>
        public string? Password { get; set; }
        /// <summary>
        /// True to enable TLS to the broker.
        /// </summary>
        public bool UseTls { get; set; }
        /// <summary>
        /// Allow untrusted SSL certificates.
        /// </summary>
        public bool AllowUntrustedCertificates { get; set; }
        /// <summary>
        /// Path to an optional cert file for a custom certificate authority.
        /// </summary>
        public string? CustomCertificateAuthority { get; set; }
        /// <summary>
        /// Mqtt client id. Should be unique for a given broker.
        /// </summary>
        [DefaultValue("cognite-opcua-extractor")]
        public string ClientId { get; set; } = "cognite-opcua-extractor";
        /// <summary>
        /// Data set to use for new objects. Existing objects will not be updated.
        /// </summary>
        public long? DataSetId { get; set; }
        /// <summary>
        /// Topic for assets.
        /// </summary>
        [DefaultValue("cognite/opcua/assets")]
        public string AssetTopic { get; set; } = "cognite/opcua/assets";
        /// <summary>
        /// Topic for timeseries.
        /// </summary>
        [DefaultValue("congite/opcua/timeseries")]
        public string TsTopic { get; set; } = "cognite/opcua/timeseries";
        /// <summary>
        /// Topic for events.
        /// </summary>
        [DefaultValue("cognite/opcua/events")]
        public string EventTopic { get; set; } = "cognite/opcua/events";
        /// <summary>
        /// Topic for datapoints.
        /// </summary>
        [DefaultValue("cognite/opcua/datapoints")]
        public string DatapointTopic { get; set; } = "cognite/opcua/datapoints";
        /// <summary>
        /// Topic for raw.
        /// </summary>
        [DefaultValue("cognite/opcua/raw")]
        public string RawTopic { get; set; } = "cognite/opcua/raw";
        /// <summary>
        /// Topic for relationships.
        /// </summary>
        [DefaultValue("cognite/opcua/relationships")]
        public string RelationshipTopic { get; set; } = "cognite/opcua/relationships";
        /// <summary>
        /// Set to enable storing a list of created assets/timeseries to local litedb.
        /// Requires the StateStorage.Location property to be set.
        /// If this is left empty, metadata will have to be read each time the extractor restarts.
        /// </summary>
        public string? LocalState { get; set; }
        /// <summary>
        /// Timestamp in ms since epoch to invalidate stored mqtt states.
        /// On extractor restart, assets/timeseries created before this will be attempted re-created in CDF.
        /// They will not be deleted or updated.
        /// </summary>
        public long InvalidateBefore { get; set; }
        /// <summary>
        /// If true, pusher will not push to target.
        /// </summary>
        public bool Debug { get; set; }
        public bool ReadExtractedRanges { get; set; }
        /// <summary>
        /// Do not push any metadata at all. If this is true, plain timeseries without metadata will be created,
        /// similarly to raw-metadata, and datapoints will be pushed. Nothing will be written to raw, and no assets will be created.
        /// Events will be created, but without asset context.
        /// </summary>
        public bool SkipMetadata { get; set; }
        /// <summary>
        /// Store assets and/or timeseries data in raw. Assets will not be created at all,
        /// timeseries will be created with just externalId, isStep and isString.
        /// Both timeseries and assets will be persisted in their entirety to raw.
        /// Datapoints are not affected, events will be created, but without asset context. The externalId
        /// of the source node is added to metadata if applicable.
        /// Use different table names for assets and timeseries.
        /// </summary>
        public RawMetadataConfig? RawMetadata { get; set; }
        /// <summary>
        /// Map metadata to asset/timeseries attributes. Each of "assets" and "timeseries" is a map from property DisplayName to
        /// CDF attribute. Legal attributes are "name, description, parentId" and "unit" for timeseries. "parentId" must somehow refer to
        /// an existing asset. For timeseries it must be a mapped asset, for assets it can be any asset.
        /// Example usage:
        /// timeseries:
        ///    "EngineeringUnits": "unit"
        ///    "EURange": "description"
        /// assets:
        ///    "Name": "name"
        /// </summary>
        public MetadataMapConfig? MetadataMapping { get; set; }
        /// <summary>
        /// Replacement for NaN values.
        /// </summary>
        public double? NonFiniteReplacement
        {
            get => nonFiniteReplacement;
            set
            {
                if (value == null) return;
                nonFiniteReplacement = double.IsFinite(value.Value) ? value : null;
            }
        }
        private double? nonFiniteReplacement;
    }
    public class FailureBufferConfig
    {
        /// <summary>
        /// Set to true to enable the failurebuffer at all.
        /// </summary>
        public bool Enabled { get; set; }
        /// <summary>
        /// If state-storage is configured, this can be used to store the ranges of points buffered in influxdb, so that
        /// they can be recovered even if the extractor goes down.
        /// </summary>
        public bool InfluxStateStore { get; set; }
        /// <summary>
        /// Use an influxdb pusher as buffer. Requires an influxdb destination to be configured.
        /// This is intended to be used if there is a local influxdb instance running.
        /// If points are received on non-historical points while the connection to CDF is down,
        /// they are read from influxdb once the connection is restored.
        /// </summary>
        public bool Influx { get; set; }
        /// <summary>
        /// Store datapoints to a binary file. There is no safety, and a bad write can corrupt the file,
        /// but it is very fast.
        /// Path to a local binary buffer file for datapoints.
        /// </summary>
        public string? DatapointPath { get; set; }
        /// <summary>
        /// Path to a local binary buffer file for events.
        /// The two buffer file paths must be different.
        /// </summary>
        public string? EventPath { get; set; }
        public long MaxBufferSize { get; set; }
    }

    public class UALoggerConfig : LoggerConfig
    {
        /// <summary>
        /// Optionally capture OPC-UA trace. One of verbose, debug, information, warning, error, fatal.
        /// </summary>
        public string? UaTraceLevel { get; set; }
        /// <summary>
        /// Try to log raw data received from and sent to OPC-UA when possible.
        /// </summary>
        public bool UaSessionTracing { get; set; }
    }
    public class FullConfig : VersionedConfig
    {
        /// <summary>
        /// Configuration for the OPC-UA server.
        /// </summary>
        public UAClientConfig Source { get; set; } = null!;
        /// <summary>
        /// Configuration for logging to console or file.
        /// </summary>
        public UALoggerConfig Logger { get; set; } = null!;
        /// <summary>
        /// Configuration for prometheus metrics.
        /// </summary>
        public UAMetricsConfig Metrics { get; set; } = null!;
        /// <summary>
        /// Configuration for writing to CDF.
        /// </summary>
        public CognitePusherConfig? Cognite { get; set; }
        /// <summary>
        /// Configuration for writing to CDF over MQTT.
        /// </summary>
        public MqttPusherConfig? Mqtt { get; set; }
        /// <summary>
        /// Configuration for writing to an influxdb server.
        /// </summary>
        public InfluxPusherConfig? Influx { get; set; }
        /// <summary>
        /// Configuration for how data should be extracted.
        /// </summary>
        public ExtractionConfig Extraction { get; set; } = null!;
        /// <summary>
        /// Configuration for how events should be extracted.
        /// </summary>
        public EventConfig Events { get; set; } = null!;
        /// <summary>
        /// Configuration for storing datapoints and events in a local buffer if
        /// connection to CDF is lost.
        /// </summary>
        public FailureBufferConfig FailureBuffer { get; set; } = null!;
        /// <summary>
        /// Configuration for reading history.
        /// </summary>
        public HistoryConfig History { get; set; } = null!;
        /// <summary>
        /// Configuration for storing the range of extracted datapoints and events to
        /// a local or remote store.
        /// </summary>
        public StateStorageConfig StateStorage { get; set; } = null!;
        /// <summary>
        /// Configuration for how to handle subscriptions to datapoints and events.
        /// </summary>
        public SubscriptionConfig Subscriptions { get; set; } = null!;
        /// <summary>
        /// Configuration for using OPC-UA pubsub to obtain datapoints and events.
        /// </summary>
        public PubSubConfig PubSub { get; set; } = null!;
        /// <summary>
        /// Configuration for high availability support.
        /// </summary>
        public HighAvailabilityConfig HighAvailability { get; set; } = null!;
        public override void GenerateDefaults()
        {
            if (Source == null) Source = new UAClientConfig();
            if (Logger == null) Logger = new UALoggerConfig();
            if (Metrics == null) Metrics = new UAMetricsConfig();
            if (Cognite != null)
            {
                if (Cognite.CdfChunking == null) Cognite.CdfChunking = new ChunkingConfig();
                if (Cognite.CdfThrottling == null) Cognite.CdfThrottling = new ThrottlingConfig();
                if (Cognite.CdfRetries == null) Cognite.CdfRetries = new RetryConfig();
                if (Cognite.SdkLogging == null) Cognite.SdkLogging = new SdkLoggingConfig();
            }
            if (Extraction == null) Extraction = new ExtractionConfig();
            if (Events == null) Events = new EventConfig();
            if (FailureBuffer == null) FailureBuffer = new FailureBufferConfig();
            if (History == null) History = new HistoryConfig();
            if (StateStorage == null) StateStorage = new StateStorageConfig();
            if (Subscriptions == null) Subscriptions = new SubscriptionConfig();
            if (PubSub == null) PubSub = new PubSubConfig();
            if (HighAvailability == null) HighAvailability = new HighAvailabilityConfig();
        }
    }
    public class EventConfig
    {
        /// <summary>
        /// Event ids to map, with full namespace-uri, and node identifier on the form "i=123" or "s=somestring"
        /// Custom events must be subtypes of the BaseEventType.
        /// This is used to specify which specific events should be extracted, instead of just extracting all events.
        /// </summary>
        public IEnumerable<ProtoNodeId>? EventIds { get; set; }
        /// <summary>
        /// Id of nodes to be observed as event emitters. Empty Namespace/NodeId defaults to the server node.
        /// This is used to add extra emitters that are not in the extracted node hierarchy, or that does not
        /// correctly specify the EventNotifier property. 
        /// </summary>
        public IEnumerable<ProtoNodeId>? EmitterIds { get; set; }
        /// <summary>
        /// Subset of the emitter-ids property. Used to make certain emitters historical.
        /// Requires the events.history property to be true
        /// </summary>
        public IEnumerable<ProtoNodeId>? HistorizingEmitterIds { get; set; }
        /// <summary>
        /// True to enable reading events from the server.
        /// </summary>
        public bool Enabled { get; set; }
        /// <summary>
        /// Automatically treat nodes with suitable EventNotifier as emitters.
        /// </summary>
        [DefaultValue(true)]
        public bool DiscoverEmitters { get; set; } = true;
        /// <summary>
        /// Enable reading both custom events and base opc-ua events.
        /// </summary>
        [DefaultValue(true)]
        public bool AllEvents { get; set; } = true;
        /// <summary>
        /// True to read event history if it is enabled.
        /// </summary>
        public bool History { get; set; }
        /// <summary>
        /// Regex filter on event type DisplayName, matches will not be extracted.
        /// </summary>
        public string? ExcludeEventFilter { get; set; }
        /// <summary>
        /// True to also check the server node when looking for event emitters, default true.
        /// </summary>
        [DefaultValue(true)]
        public bool ReadServer { get; set; } = true;
        /// <summary>
        /// List of BrowseName for properties to be excluded from automatic mapping to destination metadata.
        /// All event properties are read, by default only "Time" and "Severity" are used from the base event.
        /// Be aware that a maximum of 16 metadata entries are allowed in CDF.
        /// </summary>
        public IEnumerable<string> ExcludeProperties { get => excludeProperties; set => excludeProperties = value ?? excludeProperties; }
        private IEnumerable<string> excludeProperties = new List<string>();
        public IEnumerable<string> BaseExcludeProperties
        {
            get => baseExcludeProperties;
            set => baseExcludeProperties = value ?? baseExcludeProperties;
        }
        private IEnumerable<string> baseExcludeProperties = new List<string> { "LocalTime", "ReceiveTime" };
        /// <summary>
        /// Map source browse names to other values in the destination. For CDF, internal properties may be overwritten, by default
        /// "Message" is mapped to description, "SourceNode" is used for context and "EventType" is used for type.These may also be excluded or replaced by 
        /// overrides in DestinationNameMap. If multiple properties are mapped to the same value, the first non-null is used.
        ///
        /// If "StartTime", "EndTime" or "SubType" are specified, either directly or through the map, these are used as event properties instead of metadata.
        /// StartTime and EndTime should be either DateTime, or a number corresponding to the number of milliseconds since January 1 1970.
        /// If no StartTime or EndTime are specified, both are set to the "Time" property of BaseEventType.
        /// "Type" may be overriden case-by-case using "NameOverrides" in Extraction configuration, or in a dynamic way here. If no "Type" is specified,
        /// it is generated from Event NodeId in the same way ExternalIds are generated for normal nodes.
        /// </summary>
        public Dictionary<string, string> DestinationNameMap { get => destinationNameMap; set => destinationNameMap = value ?? destinationNameMap; }
        private Dictionary<string, string> destinationNameMap = new Dictionary<string, string>();
    }
    public class HistoryConfig
    {
        /// <summary>
        /// Enable/disable history synchronization from the OPC-UA server to CDF.
        /// This is a master switch covering both events and data
        /// </summary>
        public bool Enabled { get; set; }
        /// <summary>
        /// Enable or disable data history on nodes with history. "Enabled" must be true.
        /// By default nodes with AccessLevel ReadHistory are read.
        /// </summary>
        [DefaultValue(true)]
        public bool Data { get; set; } = true;
        /// <summary>
        /// Enable/disable backfill behavior. If this is false, data is read using frontfill only. (Pre 1.1 behavior)
        /// This applies to both datapoints and events.
        /// Backfill means that points are read backwards from the earliest known point. This means that
        /// when reading new variables, the most recent data is discovered first.
        /// </summary>
        public bool Backfill { get; set; }
        /// <summary>
        /// True to require Historizing to be set on timeseries to read history.
        /// Historizing means that the node writes history based on values written to it.
        /// </summary>
        public bool RequireHistorizing { get; set; }
        /// <summary>
        /// Max number of datapoints per variable for each history read request, 0 for server specified
        /// </summary>
        [DefaultValue(1000)]
        [Range(0, 100_000)]
        public int DataChunk { get => dataChunk; set => dataChunk = Math.Max(0, value); }
        private int dataChunk = 1000;
        /// <summary>
        /// Max number of simultaneous nodes per historyRead request for datapoints.
        /// </summary>
        [DefaultValue(100)]
        [Range(1, 10_000)]
        public int DataNodesChunk { get => dataNodesChunk; set => dataNodesChunk = Math.Max(1, value); }
        private int dataNodesChunk = 100;
        /// <summary>
        /// Maximum number of events per emitter for each per history read request, 0 for server specified.
        /// </summary>
        [DefaultValue(1000)]
        [Range(0, 100_000)]
        public int EventChunk { get => eventChunk; set => eventChunk = Math.Max(0, value); }
        private int eventChunk = 1000;
        /// <summary>
        /// Maximum number of simultaneous nodes per historyRead request for events.
        /// </summary>
        [DefaultValue(100)]
        [Range(1, 10_000)]
        public int EventNodesChunk { get => eventNodesChunk; set => eventNodesChunk = Math.Max(1, value); }
        private int eventNodesChunk = 100;

        public TimeSpanWrapper MaxReadLengthValue { get; } = new TimeSpanWrapper(true, "s", "0");
        /// <summary>
        /// Maximum length of each read of history, in seconds.
        /// If this is set greater than zero, history will be read in chunks of maximum this size, until the end.
        /// This can potentially take a very long time if end-time is much larger than start-time.
        /// Alternatively, use N[timeunit] where timeunit is w, d, h, m, s or ms.
        /// </summary>
        public string? MaxReadLength { get => MaxReadLengthValue.RawValue; set => MaxReadLengthValue.RawValue = value!; }
        /// <summary>
        /// The earliest timestamp to be read from history on the OPC-UA server, in milliseconds since 1/1/1970.
        /// Alternatively, use syntax N[timeunit](-ago) where timeunit is w, d, h, m, s or ms. In past if -ago is added,
        /// future if not.
        /// </summary>
        public string? StartTime { get; set; } = "0";
        /// <summary>
        /// Timestamp to be considered the end of forward history. Only relevant if max-read-length is set.
        /// In milliseconds since 1/1/1970. Default is current time, if this is 0.
        /// Alternatively, use syntax N[timeunit](-ago) where timeunit is w, d, h, m, s or ms. In past if -ago is added,
        /// future if not.
        /// </summary>
        public string? EndTime { get; set; }
        public TimeSpanWrapper GranularityValue { get; } = new TimeSpanWrapper(true, "s", "600");
        /// <summary>
        /// Granularity to use when doing historyRead, in seconds. Nodes with last known timestamp within this range of eachother will
        /// be read together. Should not be smaller than usual average update rate
        /// Leave at 0 to always read a single node each time.
        /// Alternatively, use N[timeunit] where timeunit is w, d, h, m, s or ms.
        /// </summary>
        [DefaultValue("600")]
        public string? Granularity { get => GranularityValue.RawValue; set => GranularityValue.RawValue = value!; }

        /// <summary>
        /// Set to true to attempt to read history without using continationPoints, instead using the Time of events, and
        /// SourceTimestamp of datapoints to incrementally change the start time of the request until no points are returned.
        /// </summary>
        public bool IgnoreContinuationPoints { get; set; }

        public CronTimeSpanWrapper RestartPeriodValue { get; } = new CronTimeSpanWrapper(false, false, "s", "0");
        /// <summary>
        /// Time in seconds to wait between each restart of history. Setting this too low may impact performance.
        /// Leave at 0 to disable periodic restarts.
        /// Alternatively, use N[timeunit] where timeunit is w, d, h, m, s or ms.
        /// You may also use a cron expression on the form "[minute] [hour] [day of month] [month] [day of week]"
        /// </summary>
        public string? RestartPeriod
        {
            get => RestartPeriodValue.RawValue; set => RestartPeriodValue.RawValue = value!;
        }
        /// <summary>
        /// Configuration for throttling history.
        /// </summary>
        public ContinuationPointThrottlingConfig Throttling
        {
            get => throttling; set => throttling = value ?? throttling;
        }
        private ContinuationPointThrottlingConfig throttling = new ContinuationPointThrottlingConfig();
        /// <summary>
        /// Log bad history datapoints, count per read at debug and each datapoint at verbose.
        /// </summary>
        [DefaultValue(true)]
        public bool LogBadValues { get; set; } = true;
    }
    public class UAThrottlingConfig
    {
        /// <summary>
        /// Maximum number of requests per minute, approximately.
        /// </summary>
        public int MaxPerMinute { get; set; }
        /// <summary>
        /// Maximum number of parallel requests.
        /// </summary>
        public int MaxParallelism { get; set; }
    }
    public class ContinuationPointThrottlingConfig : UAThrottlingConfig
    {
        /// <summary>
        /// Maximum number of nodes accross all parallel requests.
        /// </summary>
        public int MaxNodeParallelism { get; set; }
    }

    public class ProtoNodeId
    {
        /// <summary>
        /// NamespaceUri of the NodeId.
        /// </summary>
        public string? NamespaceUri { get; set; }
        /// <summary>
        /// Identifier of the NodeId, on the form i=123 or s=string, etc.
        /// </summary>
        public string? NodeId { get; set; }
        public NodeId ToNodeId(UAClient client, NodeId? defaultValue = null)
        {
            var node = client.ToNodeId(NodeId, NamespaceUri);
            if (node.IsNullNodeId)
            {
                return defaultValue ?? node;
            }
            return node;
        }
    }
    public class ProtoDataType
    {
        /// <summary>
        /// NodeId of the data type.
        /// </summary>
        public ProtoNodeId? NodeId { get; set; }
        /// <summary>
        /// True if is-step should be set on timeseries in CDF.
        /// </summary>
        public bool IsStep { get; set; }
        /// <summary>
        /// True if this is an enum.
        /// </summary>
        public bool Enum { get; set; }
    }

    public class StateStorageConfig : StateStoreConfig
    {
        public TimeSpanWrapper IntervalValue { get; } = new TimeSpanWrapper(false, "s", "0");
        /// <summary>
        /// Interval between each write to the buffer file, in seconds. 0 or less disables the state storage.
        /// Alternatively, use N[timeunit] where timeunit is w, d, h, m, s or ms.
        /// </summary>
        public string? Interval
        {
            get => IntervalValue.RawValue; set => IntervalValue.RawValue = value!;
        }
        /// <summary>
        /// Name of the raw table or litedb store for variable ranges.
        /// </summary>
        public string VariableStore { get; set; } = "variable_states";
        /// <summary>
        /// Name of the raw table or litedb store for event ranges.
        /// </summary>
        public string EventStore { get; set; } = "event_states";
        /// <summary>
        /// Name of the raw table or litedb store for influxdb failurebuffer variable ranges.
        /// </summary>
        public string InfluxVariableStore { get; set; } = "influx_variable_states";
        /// <summary>
        /// Name of the raw table or litedb store for influxdb failurebuffer event ranges.
        /// </summary>
        public string InfluxEventStore { get; set; } = "influx_event_states";
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

    public class UAMetricsConfig : MetricsConfig
    {
        /// <summary>
        /// Configuration to treat OPC-UA nodes as metrics.
        /// Values will be mapped to opcua_nodes_NODE-DISPLAY-NAME in prometheus.
        /// </summary>
        public NodeMetricsConfig? Nodes { get; set; }
    }
    public class NodeMetricsConfig
    {
        /// <summary>
        /// Map relevant static diagnostics contained in ServerDiagnosticsSummary.
        /// </summary>
        public bool ServerMetrics { get; set; }
        /// <summary>
        /// Map other nodes, given by a list of ProtoNodeIds.
        /// </summary>
        public IEnumerable<ProtoNodeId>? OtherMetrics { get; set; }
    }
    public class PubSubConfig
    {
        /// <summary>
        /// Enable pubsub.
        /// </summary>
        public bool Enabled { get; set; }
        /// <summary>
        /// Default true, prefer using the UADP binary format, if false, will prefer JSON.
        /// </summary>
        [DefaultValue(true)]
        public bool PreferUadp { get; set; } = true;
        /// <summary>
        /// Save or read configuration from a file. If the file does not exist, it will be created
        /// from server configuration. If this is pre-created manually, the server does not need to expose
        /// pubsub configuration.
        /// </summary>
        public string? FileName { get; set; }
    }
}
