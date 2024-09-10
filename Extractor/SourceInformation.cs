using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa
{
    public class SourceInformation
    {
        public string Manufacturer { get; }
        public string Name { get; }
        public string Version { get; }
        public string? Uri { get; set; }
        public DateTime? BuildDate { get; set; }

        public SourceInformation(string manufacturer, string name, string version)
        {
            Manufacturer = manufacturer;
            Name = name;
            Version = version;
        }

        public async static Task<SourceInformation?> LoadFromServer(UAClient client, ILogger logger, CancellationToken token)
        {
            try
            {
                var res = await client.ReadAttributes(new ReadValueIdCollection(
                    new[] {
                        new ReadValueId {
                            NodeId = VariableIds.Server_ServerStatus_BuildInfo,
                            AttributeId = Attributes.Value,
                        }
                    }
                ), 1, token);
                var buildInfoValue = res[0];
                if (StatusCode.IsNotGood(buildInfoValue.StatusCode)) return null;
                var buildInfo = buildInfoValue.GetValue<ExtensionObject?>(null)?.Body as BuildInfo;
                if (buildInfo == null) return null;
                return new SourceInformation(buildInfo.ManufacturerName ?? "unknown", buildInfo.ProductName ?? "unknown", buildInfo.SoftwareVersion ?? "unknown")
                {
                    Uri = buildInfo.ProductUri,
                    BuildDate = buildInfo.BuildDate,
                };
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to read build info: {Message}", ex.Message);
                return null;
            }
        }

        public static SourceInformation Default()
        {
            return new SourceInformation(
                "unknown",
                "unknown",
                "unknown"
            );
        }

        public override string ToString()
        {
            var b = new StringBuilder();
            b.AppendFormat("Name: {0}", Name);
            b.AppendFormat(", Manufacturer: {0}", Manufacturer);
            b.AppendFormat(", Version: {0}", Version);
            if (Uri != null) b.AppendFormat(", ProductUri: {0}", Uri);
            if (BuildDate != null) b.AppendFormat(", BuildDate: {0}", BuildDate);

            return b.ToString();
        }
    }
}