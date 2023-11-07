/* Cognite Extractor for OPC-UA
Copyright (C) 2023 Cognite AS

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

using Cognite.Extractor.Common;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Runtime.Serialization;
using YamlDotNet.Serialization;

namespace Cognite.OpcUa.Config
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1056:Uri properties should not be strings", Justification = "Yaml Deserialization")]
    public class SourceConfig
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
        /// Extra configuration options for redundancy
        /// </summary>
        public RedundancyConfig Redundancy { get => _redundancy; set => _redundancy = value ?? _redundancy; }
        private RedundancyConfig _redundancy = new RedundancyConfig();

        public EndpointDetails? EndpointDetails { get; set; }
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
        /// DEPRECATED, see subscriptions.sampling-interval
        /// </summary>
        [DefaultValue(100)]
        public int? SamplingInterval { get; set; } = null;
        /// <summary>
        /// DEPRECATED, see subscriptions.queue-length
        /// </summary>
        [DefaultValue(100)]
        public int? QueueLength { get; set; } = null;
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
        /// 
        /// This is overridden for the config tool.
        /// </summary>
        public UARetryConfig Retries { get => retries; set => retries = value ?? retries; }
        private UARetryConfig retries = new UARetryConfig();

        public bool IsRedundancyEnabled => EndpointUrl != null
            && AltEndpointUrls != null
            && AltEndpointUrls.Any()
            && string.IsNullOrEmpty(ReverseConnectUrl);
    }

    public class EndpointDetails
    {
        public string? OverrideEndpointUrl { get; set; }
    }

    public class UARetryConfig : RetryUtilConfig
    {
        private static uint ResolveStatusCode(string sc)
        {
            try
            {
                if (sc.StartsWith("0x"))
                {
                    return Convert.ToUInt32(sc, 16);
                }
                // If it's a number just use that
                return Convert.ToUInt32(sc);
            }
            catch
            {
                var code = StatusCodes.GetIdentifier(sc);
                if (code == 0)
                {
                    throw new ConfigurationException($"{sc} is not recognized as an OPC-UA status code. If it is a custom statuscode, use the numeric variant instead");
                }
                return code;
            }
        }

        /// <summary>
        /// List of numeric status codes to retry, in addition to a set of default codes.
        /// </summary>
        public IEnumerable<string>? RetryStatusCodes
        {
            get => retryStatusCodes?.Select(r => StatusCode.LookupSymbolicId(r))?.ToList();
            set
            {
                retryStatusCodes = value?.Select(v => ResolveStatusCode(v))?.ToList();
                finalRetryStatusCodes = new HashSet<uint>((retryStatusCodes ?? Enumerable.Empty<uint>()).Concat(internalRetryStatusCodes));
            }
        }
        private IEnumerable<uint>? retryStatusCodes;


        private readonly IEnumerable<uint> internalRetryStatusCodes = new[]
        {
            StatusCodes.BadUnexpectedError,
            StatusCodes.BadInternalError,
            StatusCodes.BadOutOfMemory,
            StatusCodes.BadResourceUnavailable,
            StatusCodes.BadCommunicationError,
            StatusCodes.BadEncodingError,
            StatusCodes.BadDecodingError,
            StatusCodes.BadUnknownResponse,
            StatusCodes.BadTimeout,
            StatusCodes.BadShutdown,
            StatusCodes.BadServerNotConnected,
            StatusCodes.BadServerHalted,
            StatusCodes.BadNothingToDo,
            StatusCodes.BadTooManyOperations,
            StatusCodes.BadSecureChannelIdInvalid,
            StatusCodes.BadNonceInvalid,
            StatusCodes.BadSessionIdInvalid,
            StatusCodes.BadSessionClosed,
            StatusCodes.BadSessionNotActivated,
            StatusCodes.BadRequestCancelledByClient,
            StatusCodes.BadNoCommunication,
            StatusCodes.BadNoContinuationPoints,
            StatusCodes.BadTooManySessions,
            StatusCodes.BadTooManyPublishRequests,
            StatusCodes.BadTcpServerTooBusy,
            StatusCodes.BadTcpSecureChannelUnknown,
            StatusCodes.BadTcpNotEnoughResources,
            StatusCodes.BadTcpInternalError,
            StatusCodes.BadRequestInterrupted,
            StatusCodes.BadRequestTimeout,
            StatusCodes.BadSecureChannelClosed,
            StatusCodes.BadDeviceFailure,
            StatusCodes.BadNotConnected,
            StatusCodes.BadSensorFailure,
            StatusCodes.BadOutOfService,
            StatusCodes.BadDisconnect,
            StatusCodes.BadConnectionClosed,
            StatusCodes.BadEndOfStream,
            StatusCodes.BadInvalidState,
            StatusCodes.BadMaxConnectionsReached,
            StatusCodes.BadConnectionRejected,
        };

        private HashSet<uint>? finalRetryStatusCodes;

        [YamlIgnore]
        public HashSet<uint> FinalRetryStatusCodes
        {
            get
            {
                if (finalRetryStatusCodes == null)
                {
                    finalRetryStatusCodes = new HashSet<uint>((retryStatusCodes ?? Enumerable.Empty<uint>()).Concat(internalRetryStatusCodes));
                    if (retryStatusCodes != null)
                    {
                        foreach (var code in retryStatusCodes) finalRetryStatusCodes.Add(code);
                    }
                }
                return finalRetryStatusCodes;
            }
        }

        private static HashSet<Type> retryExceptions = new HashSet<Type>
        {
            typeof(ArgumentNullException),
            typeof(NullReferenceException),
            typeof(InvalidOperationException)
        };

        public bool ShouldRetryException(Exception ex, IEnumerable<uint> statusCodes)
        {
            if (ex is ServiceResultException serviceExc)
            {
                var code = serviceExc.StatusCode;
                return statusCodes.Contains(serviceExc.StatusCode);
            }
            else if (ex is ServiceCallFailureException failureExc)
            {
                return failureExc.Cause == ServiceCallFailure.SessionMissing;
            }
            else if (ex is SilentServiceException silentExc)
            {
                if (silentExc.InnerServiceException != null)
                {
                    return statusCodes.Contains(silentExc.InnerServiceException.StatusCode);
                }
            }
            else if (ex is AggregateException aex)
            {
                // Only retry aggregate exceptions if one of the inner exceptions should be retried...
                var flat = aex.Flatten();
                return flat.InnerExceptions.Any(e => ShouldRetryException(e, statusCodes));
            }

            return retryExceptions.Contains(ex.GetType());
        }

        public bool ShouldRetryException(Exception ex)
        {
            return ShouldRetryException(ex, FinalRetryStatusCodes);
        }
    }

    public class RedundancyConfig
    {
        public int ServiceLevelThreshold { get; set; } = 200;
        public TimeSpanWrapper ReconnectIntervalValue { get; } = new TimeSpanWrapper(true, "m", "10m");
        public string ReconnectInterval { get => ReconnectIntervalValue.RawValue; set => ReconnectIntervalValue.RawValue = value; }
        public bool MonitorServiceLevel { get; set; }
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
}
