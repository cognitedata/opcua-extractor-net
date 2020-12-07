using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cognite.OpcUa.TypeCollectors
{
    public class BufferedObjectType
    {
        public NodeId Id { get; }
        public string Name { get; set; }
        public bool IsVariableType { get; }
        public BufferedObjectType(NodeId id, bool isVariableType)
        {
            Id = id;
            IsVariableType = isVariableType;
        }

    }
}
