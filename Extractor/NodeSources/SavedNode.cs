using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cognite.OpcUa.NodeSources
{
    public class SavedNode
    {
        public NodeId NodeId { get; set; }
        public NodeId ParentNodeId { get; set; }
        [Newtonsoft.Json.JsonProperty("name")]
        public string Name { get; set; }
        public NodeId DataTypeId { get; set; }
        public InternalInfo InternalInfo { get; set; }
    }
    public class InternalInfo
    {
        public byte EventNotifier { get; set; }
        public bool ShouldSubscribe { get; set; }
        public byte AccessLevel { get; set; }
        public bool Historizing { get; set; }
        public int ValueRank { get; set; }
        public int[] ArrayDimensions { get; set; }
        public int Index { get; set; }
        public NodeClass NodeClass { get; set; }
    }
}
