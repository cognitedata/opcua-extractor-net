using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.FDM;
using CogniteSdk.DataModels;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.Records
{
    public class LogContainerCache
    {
        private readonly Dictionary<string, Container> cache = new();
        private readonly StreamRecordsConfig config;
        private readonly CogniteDestination destination;
        private readonly ILogger log;

        private readonly Dictionary<NodeId, EventContainer> resolvedEventTypes = new();

        public LogContainerCache(CogniteDestination destination, StreamRecordsConfig config, ILogger log)
        {
            this.config = config;
            this.destination = destination;
            this.log = log;
        }

        public async Task Initialize(CancellationToken token)
        {
            string? cursor = null;
            do
            {
                var containers = await destination.CogniteClient.DataModels.ListContainers(new ContainersQuery
                {
                    Space = config.ModelSpace!,
                    Cursor = cursor,
                    Limit = 1000
                }, token);
                cursor = containers.NextCursor;
                foreach (var c in containers.Items)
                {
                    cache.TryAdd(c.ExternalId, c);
                }
            } while (cursor != null);
        }

        private BasePropertyType GetPropertyType(UAVariable prop, bool useRawNodeId)
        {
            if (prop.FullAttributes.DataType == null)
            {
                log.LogWarning("Property {Name} has unknown datatype, falling back to JSON", prop.Attributes.BrowseName);
                return BasePropertyType.Create(PropertyTypeVariant.json);
            }
            var dt = prop.FullAttributes.DataType;
            var isArray = prop.ValueRank != ValueRanks.Scalar;
            if (dt.Id == DataTypeIds.Byte
                || dt.Id == DataTypeIds.SByte
                || dt.Id == DataTypeIds.UInt16
                || dt.Id == DataTypeIds.Int16
                || dt.Id == DataTypeIds.Int32) return BasePropertyType.Create(PropertyTypeVariant.int32, isArray);
            if (dt.Id == DataTypeIds.Int64
                || dt.Id == DataTypeIds.UInt32
                || dt.Id == DataTypeIds.UInt64
                || dt.Id == DataTypeIds.UInteger
                || dt.Id == DataTypeIds.Integer) return BasePropertyType.Create(PropertyTypeVariant.int64, isArray);
            if (dt.Id == DataTypeIds.Float) return BasePropertyType.Create(PropertyTypeVariant.float32, isArray);
            if (dt.Id == DataTypeIds.Double
                || dt.Id == DataTypeIds.Duration
                || !dt.IsString) return BasePropertyType.Create(PropertyTypeVariant.float64, isArray);
            if (dt.Id == DataTypeIds.LocalizedText
                || dt.Id == DataTypeIds.QualifiedName
                || dt.Id == DataTypeIds.String
                || dt.Id == DataTypeIds.ByteString) return BasePropertyType.Text(isArray);
            if (dt.Id == DataTypeIds.DateTime
                || dt.Id == DataTypeIds.UtcTime) return BasePropertyType.Create(PropertyTypeVariant.timestamp, isArray);

            if (dt.Id == DataTypeIds.NodeId || dt.Id == DataTypeIds.ExpandedNodeId)
            {
                if (useRawNodeId)
                {
                    return BasePropertyType.Text(isArray);
                }
                else
                {
                    return BasePropertyType.Create(PropertyTypeVariant.direct);
                }
            }

            return BasePropertyType.Create(PropertyTypeVariant.json);
        }

        private ContainerCreate? BuildFromEventType(UAObjectType eventType, bool useRawNodeId)
        {
            var properties = new Dictionary<string, ContainerPropertyDefinition>();
            foreach (var field in eventType.OwnCollectedFields)
            {
                if (field.Node.NodeClass != NodeClass.Variable) continue;

                if (field.Node is not UAVariable vbProp) continue;

                var name = string.Join("_", field.BrowsePath.Select(n => FDMUtils.SanitizeExternalId(n.Name)));

                properties.Add(name, new ContainerPropertyDefinition
                {
                    Nullable = true,
                    Description = field.Node.Attributes.Description,
                    Name = field.Node.Attributes.DisplayName,
                    Type = GetPropertyType(vbProp, useRawNodeId)
                });
            }

            if (properties.Count == 0)
            {
                return null;
            }

            return new ContainerCreate
            {
                Space = config.ModelSpace,
                ExternalId = eventType.Name,
                Description = eventType.FullAttributes.Description,
                UsedFor = UsedFor.node,
                Properties = properties,
            };
        }

        public async Task EnsureContainers(IEnumerable<UAObjectType> types, bool useRawNodeId, CancellationToken token)
        {
            var missing = new Dictionary<NodeId, UAObjectType>();

            foreach (var ty in types)
            {
                if (missing.ContainsKey(ty.Id)) continue;
                var t = ty;
                while (t != null && t.Id != ObjectTypeIds.BaseObjectType)
                {
                    if (resolvedEventTypes.ContainsKey(t.Id))
                    {
                        break;
                    }
                    missing.TryAdd(t.Id, t);
                    t = t.Parent as UAObjectType;
                }
            }

            if (missing.Count == 0) return;

            var containers = new List<ContainerCreate>();
            foreach (var ty in missing.Values)
            {

                if (ty.Name == null || !cache.ContainsKey(ty.Name))
                {
                    var c = BuildFromEventType(ty, useRawNodeId);
                    if (c != null)
                    {
                        containers.Add(c);
                    }

                }
            }
            foreach (var chunk in containers.ChunkBy(100))
            {
                log.LogDebug("Create {Count} new containers for events", chunk.Count());
                var res = await destination.CogniteClient.DataModels.UpsertContainers(chunk, token);
                foreach (var c in res)
                {
                    cache.TryAdd(c.ExternalId, c);
                }
            }
        }

        public EventContainer ResolveEventType(UAObjectType eventType, CancellationToken token)
        {
            if (resolvedEventTypes.TryGetValue(eventType.Id, out var r))
            {
                return r;
            }

            EventContainer? parent = null;
            if (eventType.Parent is UAObjectType parentObjectType && eventType.Parent.Id != ObjectTypeIds.BaseObjectType)
            {
                parent = ResolveEventType(parentObjectType, token);
            }

            EventContainer res;

            if (eventType.Name is null)
            {
                log.LogWarning("Event type with ID {Id} has null name. Properties from it will not be ingested", eventType.Id);
                res = parent ?? throw new ExtractorFailureException($"Unable to resolve event type {eventType.Name} ({eventType.Id}), event type hierarchy is likely invalid");
            }
            else
            {
                if (!cache.TryGetValue(eventType.Name, out var c))
                {
                    res = parent ?? throw new ExtractorFailureException($"Unable to resolve event type {eventType.Name} ({eventType.Id}), event type hierarchy is likely invalid");
                }
                else
                {
                    res = new EventContainer(c, eventType, parent);
                }
            }

            resolvedEventTypes.TryAdd(eventType.Id, res);

            return res;
        }
    }
}
