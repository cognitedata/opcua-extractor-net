using Opc.Ua;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    class NodeSetConfig
    {
        public string FileName { get; set; }
        public Uri URL { get; set; }
    }

    public class NodeSetSource : BaseNodeSource
    {
        private readonly NodeStateCollection nodes = new NodeStateCollection();

        public NodeSetSource(FullConfig config, UAExtractor extractor, UAClient client) : base(config, extractor, client)
        {
            LoadNodeSet(new NodeSetConfig
            {
                URL = new Uri("https://files.opcfoundation.org/schemas/MTConnect/2.0/MTConnect.NodeSet2.xml")
            });
        }

        private void LoadNodeSet(NodeSetConfig set)
        {
            if (set.URL != null)
            {
                string fileName = set.URL.Segments.Last();
                if (!File.Exists(fileName))
                {
                    using (var client = new WebClient())
                    {
                        client.DownloadFile(set.URL, fileName);
                    }
                }
                set.FileName = fileName;
            }
            LoadNodeSet(set.FileName);
        }

        private void LoadNodeSet(string file)
        {
            using var stream = new FileStream(file, FileMode.Open, FileAccess.Read);
            var set = Opc.Ua.Export.UANodeSet.Read(stream);
            set.Import(Client.SystemContext, nodes);
            foreach (var node in nodes)
            {
                Console.WriteLine($"{node.NodeId}: {node.DisplayName}, {node.NodeClass}");
                var references = new List<IReference>();
                node.GetReferences(Client.SystemContext, references);
                var children = new List<BaseInstanceState>();
                node.GetChildren(Client.SystemContext, children);
                if (references.Any()) Console.WriteLine(references.Any() + ", " + string.Join(',', references.Select(reference => reference.TargetId)));
                if (children.Any()) Console.WriteLine(children.Any() + ", " + string.Join(',', children.Select(child => child.DisplayName)));
                if (node is BaseTypeState type) Console.WriteLine(type.SuperTypeId);
                if (node is BaseInstanceState instance) Console.WriteLine(instance.ModellingRuleId + ", " + instance.TypeDefinitionId);
                
            }
            foreach (var ns in Client.NamespaceTable.ToArray())
            {
                Console.WriteLine(ns);
            }
        }


        public override Task<BrowseResult> ParseResults(CancellationToken token)
        {
            throw new NotImplementedException();
        }
    }
}
