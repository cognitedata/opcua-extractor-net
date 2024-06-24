using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Net.Http.Headers;
using System.Text;

namespace Cognite.OpcUa.Types
{
    internal class DummyMessageContext : IServiceMessageContext
    {
        public DummyMessageContext(NamespaceTable namespaces)
        {
            NamespaceUris = namespaces;
        }

        public object SyncRoot => new object();

        public int MaxStringLength => 10_000;

        public int MaxArrayLength => 1000;

        public int MaxByteStringLength => 10_000;

        public int MaxMessageSize => 100_000;

        public NamespaceTable NamespaceUris { get; }

        public StringTable ServerUris { get; } = new StringTable();

        public IEncodeableFactory Factory { get; } = new EncodeableFactory();

        public int MaxDecoderRecoveries => 0;

        public int MaxEncodingNestingLevels => 200;
    }
}
