using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class PropertyNode
    {
        public string? NodeId { get; set; }
        public string? TypeDefinition { get; set; }
        public string? DataType { get; set; }
        public int[]? ArrayDimensions { get; set; }
        public int? ValueRank { get; set; }
        public string? BrowseName { get; set; }
        public int? NodeClass { get; set; }
        public bool IsMandatory { get; set; }
        public string? ReferenceType { get; set; }
        public string? DisplayName { get; set; }
        public string? ExternalId { get; set; }
    }


    public class TypeMetadata
    {
        public Dictionary<string, IEnumerable<PropertyNode>>? Properties { get; set; }
        public string? NodeId { get; set; }
        public bool IsSimple { get; set; }
        public string? Parent { get; set; }
    }
}
