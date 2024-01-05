/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

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

using Cognite.Extractor.Configuration;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Cognite.OpcUa
{
    public static class ExtractorUtils
    {
        private static readonly Dictionary<uint, string> statusCodeNames = new Dictionary<uint, string>();
        static ExtractorUtils()
        {
            var fields = typeof(StatusCodes).GetFields(BindingFlags.Public | BindingFlags.Static);
            foreach (var field in fields)
            {
                statusCodeNames.Add((uint)field.GetValue(typeof(StatusCodes)), field.Name);
            }
        }

        public static string GetStatusCodeName(uint code)
        {
            return statusCodeNames.GetValueOrDefault(code);
        }

        /// <summary>
        /// Divide a list of BufferedNodes into lists of nodes mapped to destination context objects and
        /// data variables respectively.
        /// </summary>
        /// <param name="nodes">Nodes to sort</param>
        /// <returns>Tuple of sorted objects and variables</returns>
        public static (IEnumerable<BaseUANode> objects, IEnumerable<UAVariable> variables) SortNodes(IEnumerable<BaseUANode> nodes)
        {
            if (!nodes.Any()) return (Enumerable.Empty<BaseUANode>(), Enumerable.Empty<UAVariable>());

            var timeseries = new List<UAVariable>();
            var objects = new List<BaseUANode>();
            foreach (var node in nodes)
            {
                if (node is UAVariable variable)
                {
                    if (variable.IsObject)
                    {
                        objects.Add(variable);
                    }
                    else
                    {
                        timeseries.Add(variable);
                    }
                }
                else
                {
                    objects.Add(node);
                }
            }

            return (objects, timeseries);
        }
        /// <summary>
        /// Select elements from <typeparamref name="TIn"/> to <typeparamref name="TOut"/>,
        /// returning only when the result is not null.
        /// </summary>
        /// <typeparam name="TIn">Source type</typeparam>
        /// <typeparam name="TOut">Target type</typeparam>
        /// <param name="enumerable">Source enumerable</param>
        /// <param name="map">Mapping function</param>
        /// <returns>Enumerable with non-null elements</returns>
        public static IEnumerable<TOut> SelectNonNull<TIn, TOut>(this IEnumerable<TIn> enumerable, Func<TIn, TOut?> map) where TOut : class
        {
            foreach (var item in enumerable)
            {
                var result = map(item);
                if (result == null) continue;
                yield return result;
            }
        }

        public enum SourceOp
        {
            SelectEndpoint, CreateSession, Browse, BrowseNext,
            CreateSubscription, CreateMonitoredItems, ReadAttributes, HistoryRead,
            HistoryReadEvents, ReadRootNode, DefaultOperation, CloseSession, Unknown
        }
        /// <summary>
        /// Recursively browse through aggregateException to find a root exception of given type.
        /// </summary>
        /// <typeparam name="T">Type of exception to find</typeparam>
        /// <param name="aex">AggregateException to look through</param>
        /// <returns>Null or a root exception</returns>
        public static T? GetRootExceptionOfType<T>(AggregateException aex) where T : Exception
        {
            if (aex.InnerException is T ex)
            {
                return ex;
            }
            if (aex.InnerException is AggregateException aex2)
            {
                return GetRootExceptionOfType<T>(aex2);
            }

            return null;
        }

        public static void LogException(ILogger log, Exception? e, string message, string? silentMessage = null)
        {
            if (silentMessage == null) silentMessage = message;
            if (e == null)
            {
                log.LogError("Unknown error: {Message}", message);
            }
            else if (e is AggregateException aex)
            {
                var flat = aex.Flatten();
                if (flat.InnerExceptions.Count > 1) log.LogError("{Count} errors caught", flat.InnerExceptions.Count);
                foreach (var exc in flat.InnerExceptions)
                {
                    LogException(log, exc, message, silentMessage);
                }
                if (flat.InnerExceptions.Count == 0)
                {
                    log.LogError(e, "{Message} - {ErrMessage}", message, e.Message);
                }
            }
            else if (e is SilentServiceException silent)
            {
                if (silent.InnerServiceException is not null)
                {
                    log.LogDebug("Silenced service exception: {Message} - {Info}", silentMessage, GetExcDesc(silent.InnerServiceException, silent.Operation));
                }
                else
                {
                    log.LogDebug("Silenced service exception: {Message} - {Op} {IMsg}", silentMessage,
                        silent.Operation, silent.InnerException?.Message);
                }
            }
            else if (e is ServiceResultException service)
            {
                log.LogError(e, "{Message} - {ErrMessage}: {Info}", message, service.Message, service.AdditionalInfo);
            }
            else if (e is ExtractorFailureException failure)
            {
                log.LogError("{Message} - {ErrMessage}", message, failure.Message);
                log.LogDebug(failure, "{Message}", message);
            }
            else
            {
                log.LogError(e, "{Message} - {ErrMessage}", message, e.Message);
            }
        }

        public static Exception HandleServiceResult(ILogger log, Exception ex, SourceOp op)
        {
            if (ex is AggregateException aex)
            {
                return HandleServiceResult(log, aex, op);
            }
            else if (ex is ServiceResultException serviceEx)
            {
                return HandleServiceResult(log, serviceEx, op);
            }
            else if (ex is SilentServiceException silentEx)
            {
                return silentEx;
            }
            else
            {
                log.LogError(ex, "Unexpected error of type {Type} in operation {Op}", ex.GetType(), op);
                return new SilentServiceException($"Unexpected error in operation {op}", ex, op);
            }
        }

        private static Exception HandleServiceResult(ILogger log, AggregateException ex, SourceOp op)
        {
            var exceptions = new List<Exception>();
            var flat = ex.Flatten();
            if (flat.InnerExceptions != null && flat.InnerExceptions.Count != 0)
            {
                foreach (var e in flat.InnerExceptions)
                {
                    if (e is ServiceResultException serviceEx)
                    {
                        exceptions.Add(HandleServiceResult(log, serviceEx, op));
                    }
                    else
                    {
                        exceptions.Add(e);
                    }
                }
            }
            else
            {
                return flat;
            }
            if (exceptions.Count > 1)
            {
                return new AggregateException(exceptions);
            }
            else if (exceptions.Count == 1)
            {
                return exceptions.Single();
            }
            return ex;
        }

        private static string GetExcDesc(ServiceResultException ex, SourceOp op)
        {
            var builder = new StringBuilder();
            uint code = ex.StatusCode;
            string symId = StatusCode.LookupSymbolicId(code);
            builder.AppendFormat("{0} at operation {1}.", symId, op);
            builder.AppendFormat(" Message: {0}", ex.Message);
            if (!string.IsNullOrEmpty(ex.AdditionalInfo))
            {
                builder.AppendFormat(", AdditionalInfo: {0}.", ex.AdditionalInfo);
            }
            else
            {
                builder.Append('.');
            }
            if (ex.InnerException is ServiceResultException inner)
            {
                builder.AppendFormat(" Inner: {0}", GetExcDesc(inner, op));
            }
            else if (ex.InnerException is not null)
            {
                builder.AppendFormat(" Inner: {0}, {1}", ex.InnerException.GetType(), ex.InnerException.Message);
            }
            return builder.ToString();
        }

        public static Exception HandleServiceResult(ILogger log, ServiceResultException ex, SourceOp op)
        {
            if (ex.InnerException is ServiceResultException innerServiceEx)
            {
                HandleServiceResult(log, innerServiceEx, op);
            }

            uint code = ex.StatusCode;
            string symId = StatusCode.LookupSymbolicId(code);

            switch (code)
            {
                // Handle common errors
                case StatusCodes.BadDecodingError:
                case StatusCodes.BadUnknownResponse:
                    // This really shouldn't happen, it is either some freak communication error or an issue with the server
                    log.LogError("Server responded with bad data: {Message}", GetExcDesc(ex, op));
                    log.LogError("This is unlikely to be an issue with the extractor");
                    log.LogError("If it repeats, it is most likely a bug in the server");
                    return new SilentServiceException("Server responded with bad data", ex, op);
                case StatusCodes.BadCertificateChainIncomplete:
                case StatusCodes.BadCertificateHostNameInvalid:
                case StatusCodes.BadCertificateInvalid:
                case StatusCodes.BadCertificateIssuerRevocationUnknown:
                case StatusCodes.BadCertificateIssuerRevoked:
                case StatusCodes.BadCertificateIssuerTimeInvalid:
                case StatusCodes.BadCertificateIssuerUseNotAllowed:
                case StatusCodes.BadCertificatePolicyCheckFailed:
                case StatusCodes.BadCertificateRevocationUnknown:
                case StatusCodes.BadCertificateRevoked:
                    log.LogError("There was an issue with the certificate: {Message}", GetExcDesc(ex, op));
                    return new SilentServiceException("There was an issue with the certificate", ex, op);
                case StatusCodes.BadNothingToDo:
                    log.LogError("Server had nothing to do, this is likely an issue with the extractor: {Message}", GetExcDesc(ex, op));
                    return new SilentServiceException("Server had nothing to do", ex, op);
                case StatusCodes.BadSessionClosed:
                    // This sometimes occurs if the client is closed during an operation, it is expected
                    log.LogError("Service failed due to closed Session: {Message}", GetExcDesc(ex, op));
                    return new SilentServiceException("Service failed due to closed Session", ex, op);
                case StatusCodes.BadServerNotConnected:
                    log.LogError("The client attempted a connection without being connected to the server: {Message}", GetExcDesc(ex, op));
                    log.LogError("This is most likely an issue with the extractor");
                    return new SilentServiceException("Attempted call to unconnected server", ex, op);
                case StatusCodes.BadServerHalted:
                    log.LogError("Server halted unexpectedly: {Message}", GetExcDesc(ex, op));
                    return new SilentServiceException("Server stopped unexpectedly", ex, op);
                case StatusCodes.BadRequestInterrupted:
                    log.LogError("Failed to send request. The request size might be too large for the server: {Message}", GetExcDesc(ex, op));
                    return new SilentServiceException("Failed to send request to server", ex, op);
                case StatusCodes.BadRequestTooLarge:
                    log.LogError("Failed to send request due to too large request size: {Message}", GetExcDesc(ex, op));
                    log.LogError("This might be solvable by increasing request limits in the xml config file, or by reducing chunk sizes");
                    return new SilentServiceException("Too large request", ex, op);
                case StatusCodes.BadSecureChannelClosed:
                case StatusCodes.BadConnectionClosed:
                    log.LogError("Failed to send request due to loss of connection to the server: {Message}", GetExcDesc(ex, op));
                    return new SilentServiceException("Failed to send request", ex, op);
                default:
                    switch (op)
                    {
                        case SourceOp.SelectEndpoint:
                            if (code == StatusCodes.BadNotConnected || code == StatusCodes.BadSecureChannelClosed)
                            {
                                // The most common error, generally happens if the server cannot be found
                                log.LogError("Unable to connect to discovery server: {Message}", GetExcDesc(ex, op));
                                log.LogError("Check the EndpointURL, and make sure that the server is accessible");
                                return new SilentServiceException("Unable to connect to discovery server", ex, op);
                            }
                            break;
                        case SourceOp.CreateSession:
                            switch (code)
                            {
                                case StatusCodes.BadIdentityTokenInvalid:
                                    log.LogError("Invalid identity token, most likely a configuration issue: {Message}", GetExcDesc(ex, op));
                                    log.LogError("Make sure that the username and password given are valid");
                                    return new SilentServiceException("Invalid identity token", ex, op);
                                case StatusCodes.BadIdentityTokenRejected:
                                    log.LogError("Identity token rejected, most likely incorrect username or password: {Message}", GetExcDesc(ex, op));
                                    return new SilentServiceException("Identity token rejected", ex, op);
                                case StatusCodes.BadCertificateUntrusted:
                                    log.LogError("Certificate not trusted by server: {Message}", GetExcDesc(ex, op));
                                    log.LogError("This can be fixed by moving trusting the certificate on the server");
                                    return new SilentServiceException("Certificate untrusted", ex, op);
                            }
                            break;
                        case SourceOp.ReadRootNode:
                            if (code == StatusCodes.BadNodeIdInvalid || code == StatusCodes.BadNodeIdUnknown)
                            {
                                log.LogError("Root node not found, check configuration: {Message}", GetExcDesc(ex, op));
                                return new SilentServiceException("Root node not found", ex, op);
                            }
                            goto case SourceOp.ReadAttributes;
                        case SourceOp.Browse:
                            switch (code)
                            {
                                case StatusCodes.BadNodeIdInvalid:
                                case StatusCodes.BadNodeIdUnknown:
                                case StatusCodes.BadReferenceTypeIdInvalid:
                                case StatusCodes.BadBrowseDirectionInvalid:
                                    log.LogError("Error during browse, this is most likely a limitation of the server: {Message}", GetExcDesc(ex, op));
                                    return new SilentServiceException("Unexpected error during Browse", ex, op);
                            }
                            goto case SourceOp.DefaultOperation;
                        case SourceOp.BrowseNext:
                            if (code == StatusCodes.BadServiceUnsupported)
                            {
                                log.LogError("BrowseNext not supported by server: {Message}", GetExcDesc(ex, op));
                                log.LogError("This is a required service, but it may be possible to increase browse chunk sizes to avoid the issue");
                                return new SilentServiceException("BrowseNext unspported", ex, op);
                            }
                            goto case SourceOp.Browse;
                        case SourceOp.ReadAttributes:
                            switch (code)
                            {
                                case StatusCodes.BadNodeIdInvalid:
                                case StatusCodes.BadNodeIdUnknown:
                                case StatusCodes.BadAttributeIdInvalid:
                                case StatusCodes.BadNotReadable:
                                    log.LogError("Failure during read, this is most likely a limitation of the server: {Message}", GetExcDesc(ex, op));
                                    return new SilentServiceException("Unexpected error during Read", ex, op);
                                case StatusCodes.BadUserAccessDenied:
                                    log.LogError("Failed to read attributes due to insufficient access rights: {Message}", GetExcDesc(ex, op));
                                    return new SilentServiceException("User access denied during Read", ex, op);
                                case StatusCodes.BadSecurityModeInsufficient:
                                    log.LogError("Failed to read attributes due to insufficient security level: {Message}", GetExcDesc(ex, op));
                                    log.LogError("This generally means that reading of specific attributes/nodes requires a secure connection" +
                                              ", and the current connection is not sufficiently secure");
                                    return new SilentServiceException("Insufficient security during Read", ex, op);
                            }
                            goto case SourceOp.DefaultOperation;
                        case SourceOp.CreateSubscription:
                            switch (code)
                            {
                                case StatusCodes.BadTooManySubscriptions:
                                    log.LogError("Too many subscriptions on server: {Message}", GetExcDesc(ex, op));
                                    log.LogError("The extractor creates a maximum of three subscriptions, one for data, one for events, one for auditing");
                                    log.LogError("If this happens after multiple reconnects, it may be due to poor reconnect handling somewhere, " +
                                              "in that case, it may help to turn on ForceRestart in order to clean up subscriptions between each reconnect");
                                    return new SilentServiceException("Too many subscriptions", ex, op);
                                case StatusCodes.BadServiceUnsupported:
                                    log.LogError("Create subscription unsupported by server: {Message}", GetExcDesc(ex, op));
                                    log.LogError("This may be an issue with the extractor, or more likely a server limitation");
                                    return new SilentServiceException("CreateSubscription unsupported", ex, op);
                            }
                            // Creating a subscription in the SDK also involves a call to the CreateMonitoredItems service, usually
                            goto case SourceOp.CreateMonitoredItems;
                        case SourceOp.CreateMonitoredItems:
                            switch (code)
                            {
                                case StatusCodes.BadSubscriptionIdInvalid:
                                    log.LogError("Subscription not found on server: {Message}", GetExcDesc(ex, op));
                                    log.LogError("This is generally caused by a desync between the server and the client");
                                    log.LogError("A solution may be to turn on ForceRestart, to clean up subscriptions between each connect");
                                    return new SilentServiceException("Subscription id invalid", ex, op);
                                case StatusCodes.BadFilterNotAllowed:
                                case StatusCodes.BadFilterOperatorUnsupported:
                                case StatusCodes.BadFilterOperandInvalid:
                                case StatusCodes.BadFilterLiteralInvalid:
                                case StatusCodes.BadEventFilterInvalid:
                                    log.LogError("Event filter invalid: {Message}", GetExcDesc(ex, op));
                                    log.LogError("This may be an issue with the extractor, or the server may not fully support event filtering");
                                    return new SilentServiceException("Filter related error", ex, op);
                                case StatusCodes.BadTooManyMonitoredItems:
                                    log.LogError("Server has reached limit of monitored items: {Message}", GetExcDesc(ex, op));
                                    log.LogError("The extractor requires one monitored item per data variable, and one per configured event emitter node");
                                    log.LogError("If this happens after multiple reconnects it may be due to poor reconnect handling somewhere, " +
                                              "in that case, it may help to turn on ForceRestarts in order to clean up subscriptions between each reconnect");
                                    return new SilentServiceException("Too many monitoredItems", ex, op);
                            }
                            goto case SourceOp.DefaultOperation;
                        case SourceOp.HistoryRead:
                            switch (code)
                            {
                                case StatusCodes.BadNodeIdInvalid:
                                case StatusCodes.BadNodeIdUnknown:
                                case StatusCodes.BadDataEncodingInvalid:
                                case StatusCodes.BadDataEncodingUnsupported:
                                    log.LogError("Failure during HistoryRead, this may be caused by a server limitation: {Message}", GetExcDesc(ex, op));
                                    return new SilentServiceException("Unexpected error in HistoryRead", ex, op);
                                case StatusCodes.BadUserAccessDenied:
                                    log.LogError("Failed to read History due to insufficient access rights: {Message}", GetExcDesc(ex, op));
                                    return new SilentServiceException("User access denied during HistoryRead", ex, op);
                                case StatusCodes.BadTooManyOperations:
                                    log.LogError("Failed to read History due to too many operations: {Message}", GetExcDesc(ex, op));
                                    log.LogError("This may be due to too large chunk sizes, try to lower chunk sizes for {Op}", op);
                                    return new SilentServiceException("Too many operations during HistoryRead", ex, op);
                                case StatusCodes.BadHistoryOperationUnsupported:
                                case StatusCodes.BadHistoryOperationInvalid:
                                    log.LogError("HistoryRead operation unsupported by server: {Message}", GetExcDesc(ex, op));
                                    log.LogError("The extractor uses HistoryReadRaw for data and HistoryReadEvents for events");
                                    log.LogError("If the server does not support one, they may be disabled individually");
                                    return new SilentServiceException("HistoryRead operation unspported", ex, op);
                            }

                            break;
                        case SourceOp.HistoryReadEvents:
                            switch (code)
                            {
                                case StatusCodes.BadFilterNotAllowed:
                                case StatusCodes.BadFilterOperatorUnsupported:
                                case StatusCodes.BadFilterOperandInvalid:
                                case StatusCodes.BadFilterLiteralInvalid:
                                case StatusCodes.BadEventFilterInvalid:
                                    log.LogError("Event filter invalid: {Message}", GetExcDesc(ex, op));
                                    log.LogError("This may be an issue with the extractor, or the server may not fully support event filtering");
                                    return new SilentServiceException("Filter related error", ex, op);
                            }
                            goto case SourceOp.HistoryRead;
                        case SourceOp.DefaultOperation:
                            switch (code)
                            {
                                case StatusCodes.BadServiceUnsupported:
                                    log.LogError("Base requirement \"{Op}\" unspported by server: {Code}", op, symId);
                                    log.LogError("This is a required service, if the server does not support it the extractor may not be used");
                                    return new SilentServiceException($"{op} unsupported", ex, op);
                                case StatusCodes.BadNoContinuationPoints:
                                    log.LogError("Server is out of continuationPoints, this may be the " +
                                              "result of poor configuration of the extractor: {Message}", GetExcDesc(ex, op));
                                    log.LogError("If the chunk sizes for {Op} are set very low, that may be the cause", op);
                                    return new SilentServiceException($"Too many continuationPoints for {op}", ex, op);
                                case StatusCodes.BadTooManyOperations:
                                    log.LogError("Too many operations, this is most likely due to chunkSize being set too high: {Message}", GetExcDesc(ex, op));
                                    log.LogError("Try lowering the chunk sizes for {Op}", op);
                                    return new SilentServiceException($"Too many operations for {op}", ex, op);
                            }
                            break;
                        case SourceOp.CloseSession:
                            log.LogError("Failed to close session, this is almost always due to the session already being closed: {Message}", GetExcDesc(ex, op));
                            return new SilentServiceException("Failed to close session", ex, op);
                    }
                    log.LogError(ex, "Unexpected service result exception in operation {Message}", GetExcDesc(ex, op));
                    return new SilentServiceException("Unexpected error", ex, op);
            }
        }

        /// <summary>
        /// Intelligently converts an instance of FullConfig to a string config file. Only writing entries that differ from the default values.
        /// </summary>
        /// <param name="config">Config to convert</param>
        /// <returns>Final config string, can be written directly to file or parsed further</returns>
        public static string ConfigToString(FullConfig config)
        {
            return ConfigurationUtils.ConfigToString(config,
                Enumerable.Empty<string>(),
                new[] { "ConfigDir", "BaseExcludeProperties", "IdpAuthentication", "ApiKey", "Password" },
                new[] { "Cognite" },
                false);
        }

        public static string GetValueRankString(int valueRank)
        {
            return valueRank switch
            {
                ValueRanks.Any => "Any",
                ValueRanks.OneDimension => "OneDimension",
                ValueRanks.OneOrMoreDimensions => "OneOrMoreDimensions",
                ValueRanks.Scalar => "Scalar",
                ValueRanks.ScalarOrOneDimension => "ScalarOrOneDimension",
                ValueRanks.TwoDimensions => "TwoDimensions",
                _ => $"{valueRank}Dimensions",
            };
        }
    }

    /// <summary>
    /// Used to indicate a serviceException that has been recognized and properly logged.
    /// </summary>
    public class SilentServiceException : Exception
    {
        public ExtractorUtils.SourceOp Operation { get; }
        public uint StatusCode { get; }
        public ServiceResultException? InnerServiceException { get; }

        private static string GetSymbolicId(uint sc)
        {
            return Opc.Ua.StatusCode.LookupSymbolicId(sc);
        }

        public SilentServiceException(string msg, ServiceResultException ex, ExtractorUtils.SourceOp op)
            : base($"{msg}: code {GetSymbolicId(ex?.StatusCode ?? StatusCodes.BadUnexpectedError)}, operation {op}", ex)
        {
            Operation = op;
            StatusCode = ex?.StatusCode ?? StatusCodes.BadUnexpectedError;
            InnerServiceException = ex;
        }

        public SilentServiceException(string msg, Exception ex, ExtractorUtils.SourceOp op)
            : base($"{msg}: operation {op}", ex)
        {
            Operation = op;
            StatusCode = StatusCodes.BadUnexpectedError;
        }

        public SilentServiceException()
        {
            Operation = ExtractorUtils.SourceOp.Unknown;
            StatusCode = StatusCodes.Bad;
        }

        public SilentServiceException(string message) : base(message)
        {
            Operation = ExtractorUtils.SourceOp.Unknown;
            StatusCode = StatusCodes.Bad;
        }

        public SilentServiceException(string message, Exception innerException) : base(message, innerException)
        {
            Operation = ExtractorUtils.SourceOp.Unknown;
            StatusCode = StatusCodes.Bad;
            InnerServiceException = innerException as ServiceResultException;
        }
    }
    /// <summary>
    /// Used to indicate that an exception was thrown due to some controlled failure of the extractor.
    /// </summary>
    public class ExtractorFailureException : Exception
    {
        public ExtractorFailureException(string msg) : base(msg) { }
        public ExtractorFailureException() { }

        public ExtractorFailureException(string message, Exception innerException) : base(message, innerException) { }
    }
    /// <summary>
    /// Indicates a fatal error in some system
    /// </summary>
    public class FatalException : Exception
    {
        public FatalException(string message) : base(message) { }

        public FatalException(string message, Exception innerException) : base(message, innerException) { }

        public FatalException() { }
    }

    public enum ServiceCallFailure
    {
        SessionMissing,
        ServerFault,
        ExtractorError
    }

    public class ServiceCallFailureException : Exception
    {
        public ServiceCallFailure Cause { get; }
        public ServiceCallFailureException(string message, ServiceCallFailure cause, Exception innerException) : base(message, innerException)
        {
            Cause = cause;
        }

        public ServiceCallFailureException(string message, ServiceCallFailure cause) : base(message)
        {
            Cause = cause;
        }

        public ServiceCallFailureException() : this("Unspecified extractor error", ServiceCallFailure.ExtractorError)
        {
        }

        public ServiceCallFailureException(string message) : this(message, ServiceCallFailure.ExtractorError)
        {
        }

        public ServiceCallFailureException(string message, Exception innerException) : this(message, ServiceCallFailure.ExtractorError, innerException)
        {
        }
    }
}
