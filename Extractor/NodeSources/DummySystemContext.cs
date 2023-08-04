using Opc.Ua;
using System.Collections.Generic;

namespace Cognite.OpcUa.NodeSources
{
    public class DummySystemContext : ISystemContext, INodeIdFactory
    {
        public DummySystemContext(NamespaceTable referenceTable)
        {
            NamespaceUris = referenceTable;
            TypeTable = new TypeTable(NamespaceUris);
        }

        public object SystemHandle => this;
        public NodeId SessionId => NodeId.Null;
        public IUserIdentity UserIdentity => new UserIdentity(new AnonymousIdentityToken());
        public IList<string> PreferredLocales => new List<string>();
        public string? AuditEntryId => null;
        public NamespaceTable NamespaceUris { get; }
        public StringTable ServerUris => new StringTable();
        public ITypeTable TypeTable { get; }
        public IEncodeableFactory EncodeableFactory => new EncodeableFactory();
        public INodeIdFactory NodeIdFactory => this;
        public NodeStateFactory NodeStateFactory => new NodeStateFactory();

        public NodeId New(ISystemContext context, NodeState node)
        {
            return node.NodeId;
        }
    }
}
