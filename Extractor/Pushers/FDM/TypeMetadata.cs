using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class PropertyMetadata
    {
        public IEnumerable<string>? BrowsePath { get; set; }
        public string? NodeId { get; set; }
        public string? DataType { get; set; }
        public string? TypeDefinition { get; set; }
        public int[]? ArrayDimensions { get; set; }
        public int? ValueRank { get; set; }
    }

    public class TypeMetadata
    {
        public Dictionary<string, PropertyMetadata>? Properties { get; set; }
        public string? NodeId { get; set; }
        public bool IsSimple { get; set; }
        public string? Parent { get; set; }
    }
}
