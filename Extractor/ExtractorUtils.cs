﻿/* Cognite Extractor for OPC-UA
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

using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa
{
    public static class ExtractorUtils
    {
        /// <summary>
        /// Divide a list of BufferedNodes into lists of nodes mapped to destination context objects and
        /// data variables respectively.
        /// </summary>
        /// <param name="nodes">Nodes to sort</param>
        /// <returns>Tuple of sorted objects and variables</returns>
        public static (IEnumerable<UANode> objects, IEnumerable<UAVariable> variables) SortNodes(IEnumerable<UANode> nodes)
        {
            if (!nodes.Any()) return (Enumerable.Empty<UANode>(), Enumerable.Empty<UAVariable>());

            var timeseries = new List<UAVariable>();
            var objects = new List<UANode>();
            foreach (var node in nodes)
            {
                if (node is UAVariable variable)
                {
                    if (variable.IsArray && variable.Index == -1)
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
        /// Select elements from <typeparamref name="R"/> to <typeparamref name="T"/>,
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

        public static void LogException(Microsoft.Extensions.Logging.ILogger log, Exception? e, string message, string silentMessage)
        {
            if (e == null)
            {
                log.LogError("{msg}", message);
            }
            else if (e is AggregateException aex)
            {
                var flat = aex.Flatten();
                foreach (var exc in flat.InnerExceptions)
                {
                    LogException(log, exc, message, silentMessage);
                }
                if (!flat.InnerExceptions.Any())
                {
                    log.LogError(e, "{pMsg} - {msg}", message, e.Message);
                }
            }
            else if (e is SilentServiceException silent)
            {
                log.LogDebug("Silenced service exception: {msg} - {info}", silentMessage,
                    silent.InnerServiceException?.AdditionalInfo);
            }
            else if (e is ServiceResultException service)
            {
                log.LogError(e, "{pMsg} - {msg}: {info}", message, service.Message, service.AdditionalInfo);
            }
            else if (e is ExtractorFailureException failure)
            {
                log.LogError("{pMsg} - {msg}", message, failure.Message);
                log.LogDebug(failure, "{msg}", message);
            }
            else
            {
                log.LogError(e, "{pMsg} - {msg}", e.Message);
            }
        }


        /// <summary>
        /// Log exception, silencing SilentServiceExceptions and formatting results properly.
        /// </summary>
        /// <param name="e">Exception to log</param>
        /// <param name="message">Message to give with normal exceptions</param>
        /// <param name="silentMessage">Message to give with silent exceptions</param>
        public static void LogException(Serilog.ILogger log, Exception? e, string message, string? silentMessage = null)
        {
            if (silentMessage == null) silentMessage = message;
            if (e == null)
            {
                log.Error(message);
            }
            else if (e is AggregateException aex)
            {
                var flat = aex.Flatten();
                foreach (var exc in flat.InnerExceptions)
                {
                    LogException(log, exc, message, silentMessage);
                }
                if (!flat.InnerExceptions.Any())
                {
                    log.Error(e, message + " - {msg}", e.Message);
                }
            }
            else if (e is SilentServiceException silent)
            {
                log.Debug("Silenced service exception: {msg} - {info}", silentMessage,
                    silent.InnerServiceException?.AdditionalInfo);
            }
            else if (e is ServiceResultException service)
            {
                log.Error(e, message + " - {msg}: {info}", service.Message, service.AdditionalInfo);
            }
            else if (e is ExtractorFailureException failure)
            {
                log.Error(message + " - {msg}", failure.Message);
                log.Debug(failure, message);
            }
            else if (e is FatalException fatal)
            {
                log.Fatal(message + " - {msg}", fatal.Message);
                log.Debug(fatal, message);
            }
            else
            {
                log.Error(e, message + " - {msg}", e.Message);
            }
        }
        public static Exception HandleServiceResult(Serilog.ILogger log, Exception ex, SourceOp op)
        {
            if (ex is AggregateException aex)
            {
                return HandleServiceResult(log, aex, op);
            }
            else if (ex is ServiceResultException serviceEx)
            {
                return HandleServiceResult(log, serviceEx, op);
            }
            else
            {
                log.Error(ex, "Unexpected error of type {type} in operation {op}", ex.GetType(), op);
                return new SilentServiceException($"Unexpected error in operation {op}", ex, op);
            }
        }

        private static Exception HandleServiceResult(Serilog.ILogger log, AggregateException ex, SourceOp op)
        {
            var exceptions = new List<Exception>();
            var flat = ex.Flatten();
            if (flat.InnerExceptions != null && flat.InnerExceptions.Any())
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

        /// <summary>
        /// Parse ServiceResult from OPC-UA and log then transform it into a
        /// SilentServiceException if it is recognized, or just return it if not.
        /// </summary>
        /// <param name="ex">Exception to transform</param>
        /// <param name="op">Source operation, for logging</param>
        /// <returns>Transformed exception if recognized, otherwise the given exception</returns>
        private static Exception HandleServiceResult(Serilog.ILogger log, ServiceResultException ex, SourceOp op)
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
                    log.Error("Server responded with bad data: {code}, at operation {op}", symId, op);
                    log.Error("This is unlikely to be an issue with the extractor");
                    log.Error("If it repeats, it is most likely a bug in the server");
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
                    log.Error("There was an issue with the certificate: {code} at operation {op}", symId, op);
                    return new SilentServiceException("There was an issue with the certificate", ex, op);
                case StatusCodes.BadNothingToDo:
                    log.Error("Server had nothing to do, this is likely an issue with the extractor: {code} at operation {op}",
                        symId, op);
                    return new SilentServiceException("Server had nothing to do", ex, op);
                case StatusCodes.BadSessionClosed:
                    // This sometimes occurs if the client is closed during an operation, it is expected
                    log.Error("Service failed due to closed Session: {code} at operation {op}", symId, op);
                    return new SilentServiceException("Service failed due to closed Session", ex, op);
                case StatusCodes.BadServerNotConnected:
                    log.Error("The client attempted a connection without being connected to the server: {code} at operation {op}",
                        symId, op);
                    log.Error("This is most likely an issue with the extractor");
                    return new SilentServiceException("Attempted call to unconnected server", ex, op);
                case StatusCodes.BadServerHalted:
                    log.Error("Server halted unexpectedly: {code} at operation {op}", symId, op);
                    return new SilentServiceException("Server stopped unexpectedly", ex, op);
                case StatusCodes.BadRequestInterrupted:
                    log.Error("Failed to send request. The request size might be too large for the server: {code} at operation {op}",
                        symId, op);
                    return new SilentServiceException("Failed to send request to server", ex, op);
                case StatusCodes.BadRequestTooLarge:
                    log.Error("Failed to send request due to too large request size: {code} at operation {op}", symId, op);
                    log.Error("This might be solvable by increasing request limits in the xml config file, or by reducing chunk sizes");
                    return new SilentServiceException("Too large request", ex, op);
                default:
                    switch (op)
                    {
                        case SourceOp.SelectEndpoint:
                            if (code == StatusCodes.BadNotConnected || code == StatusCodes.BadSecureChannelClosed)
                            {
                                // The most common error, generally happens if the server cannot be found
                                log.Error("Unable to connect to discovery server: {code} at operation {op}",
                                    symId, op.ToString());
                                log.Error("Check the EndpointURL, and make sure that the server is accessible");
                                return new SilentServiceException("Unable to connect to discovery server", ex, op);
                            }
                            break;
                        case SourceOp.CreateSession:
                            switch (code)
                            {
                                case StatusCodes.BadIdentityTokenInvalid:
                                    log.Error("Invalid identity token, most likely a configuration issue: {code} at operation {op}",
                                        symId, op.ToString());
                                    log.Error("Make sure that the username and password given are valid");
                                    return new SilentServiceException("Invalid identity token", ex, op);
                                case StatusCodes.BadIdentityTokenRejected:
                                    log.Error("Identity token rejected, most likely incorrect username or password: {code} at operation {op}",
                                        symId, op.ToString());
                                    return new SilentServiceException("Identity token rejected", ex, op);
                                case StatusCodes.BadCertificateUntrusted:
                                    log.Error("Certificate not trusted by server: {code} at operation {op}", symId, op);
                                    log.Error("This can be fixed by moving trusting the certificate on the server");
                                    return new SilentServiceException("Certificate untrusted", ex, op);
                            }
                            break;
                        case SourceOp.ReadRootNode:
                            if (code == StatusCodes.BadNodeIdInvalid || code == StatusCodes.BadNodeIdUnknown)
                            {
                                log.Error("Root node not found, check configuration: {code} at operation {op}",
                                    symId, op.ToString());
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
                                    log.Error("Error during browse, this is most likely a limitation of the server: {code} at operation {op}",
                                        symId, op.ToString());
                                    return new SilentServiceException("Unexpected error during Browse", ex, op);
                            }
                            goto case SourceOp.DefaultOperation;
                        case SourceOp.BrowseNext:
                            if (code == StatusCodes.BadServiceUnsupported)
                            {
                                log.Error("BrowseNext not supported by server: {code} at operation {op}", symId, op);
                                log.Error("This is a required service, but it may be possible to increase browse chunk sizes to avoid the issue");
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
                                    log.Error("Failure during read, this is most likely a limitation of the server: {code} at operation {op}",
                                        symId, op.ToString());
                                    return new SilentServiceException("Unexpected error during Read", ex, op);
                                case StatusCodes.BadUserAccessDenied:
                                    log.Error("Failed to read attributes due to insufficient access rights: {code} at operation {op}",
                                        symId, op.ToString());
                                    return new SilentServiceException("User access denied during Read", ex, op);
                                case StatusCodes.BadSecurityModeInsufficient:
                                    log.Error("Failed to read attributes due to insufficient security level: {code} at operation {op}",
                                        symId, op.ToString());
                                    log.Error("This generally means that reading of specific attributes/nodes requires a secure connection" +
                                              ", and the current connection is not sufficiently secure");
                                    return new SilentServiceException("Insufficient security during Read", ex, op);
                            }
                            goto case SourceOp.DefaultOperation;
                        case SourceOp.CreateSubscription:
                            switch (code)
                            {
                                case StatusCodes.BadTooManySubscriptions:
                                    log.Error("Too many subscriptions on server: {code} at operation {op}", symId, op);
                                    log.Error("The extractor creates a maximum of three subscriptions, one for data, one for events, one for auditing");
                                    log.Error("If this happens after multiple reconnects, it may be due to poor reconnect handling somewhere, " +
                                              "in that case, it may help to turn on ForceRestart in order to clean up subscriptions between each reconnect");
                                    return new SilentServiceException("Too many subscriptions", ex, op);
                                case StatusCodes.BadServiceUnsupported:
                                    log.Error("Create subscription unsupported by server: {code} at operation {op}", symId, op);
                                    log.Error("This may be an issue with the extractor, or more likely a server limitation");
                                    return new SilentServiceException("CreateSubscription unsupported", ex, op);
                            }
                            // Creating a subscription in the SDK also involves a call to the CreateMonitoredItems service, usually
                            goto case SourceOp.CreateMonitoredItems;
                        case SourceOp.CreateMonitoredItems:
                            switch (code)
                            {
                                case StatusCodes.BadSubscriptionIdInvalid:
                                    log.Error("Subscription not found on server", symId, op);
                                    log.Error("This is generally caused by a desync between the server and the client");
                                    log.Error("A solution may be to turn on ForceRestart, to clean up subscriptions between each connect");
                                    return new SilentServiceException("Subscription id invalid", ex, op);
                                case StatusCodes.BadFilterNotAllowed:
                                case StatusCodes.BadFilterOperatorUnsupported:
                                case StatusCodes.BadFilterOperandInvalid:
                                case StatusCodes.BadFilterLiteralInvalid:
                                case StatusCodes.BadEventFilterInvalid:
                                    log.Error("Event filter invalid: {code} at operation {op}", symId, op);
                                    log.Error("This may be an issue with the extractor, or the server may not fully support event filtering");
                                    return new SilentServiceException("Filter related error", ex, op);
                                case StatusCodes.BadTooManyMonitoredItems:
                                    log.Error("Server has reached limit of monitored items", symId, op);
                                    log.Error("The extractor requires one monitored item per data variable, and one per configured event emitter node");
                                    log.Error("If this happens after multiple reconnects it may be due to poor reconnect handling somewhere, " +
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
                                    log.Error("Failure during HistoryRead, this may be caused by a server limitation: {code} at operation {op}",
                                        symId, op.ToString());
                                    return new SilentServiceException("Unexpected error in HistoryRead", ex, op);
                                case StatusCodes.BadUserAccessDenied:
                                    log.Error("Failed to read History due to insufficient access rights: {code} at operation {op}",
                                        symId, op.ToString());
                                    return new SilentServiceException("User access denied during HistoryRead", ex, op);
                                case StatusCodes.BadTooManyOperations:
                                    log.Error("Failed to read History due to too many operations: {code} at operation {op}",
                                        symId, op.ToString());
                                    log.Error("This may be due to too large chunk sizes, try to lower chunk sizes for {op}", op);
                                    return new SilentServiceException("Too many operations during HistoryRead", ex, op);
                                case StatusCodes.BadHistoryOperationUnsupported:
                                case StatusCodes.BadHistoryOperationInvalid:
                                    log.Error("HistoryRead operation unsupported by server: {code} at operation {op}");
                                    log.Error("The extractor uses HistoryReadRaw for data and HistoryReadEvents for events");
                                    log.Error("If the server does not support one, they may be disabled individually");
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
                                    log.Error("Event filter invalid: {code} at operation {op}", symId, op);
                                    log.Error("This may be an issue with the extractor, or the server may not fully support event filtering");
                                    return new SilentServiceException("Filter related error", ex, op);
                            }
                            goto case SourceOp.HistoryRead;
                        case SourceOp.DefaultOperation:
                            switch (code)
                            {
                                case StatusCodes.BadServiceUnsupported:
                                    log.Error("Base requirement \"{op}\" unspported by server: {code}", op, symId);
                                    log.Error("This is a required service, if the server does not support it the extractor may not be used");
                                    return new SilentServiceException($"Unsupported operation", ex, op);
                                case StatusCodes.BadNoContinuationPoints:
                                    log.Error("Server is out of continuationPoints, this may be the " +
                                              "result of poor configuration of the extractor: {code} at operation {op}",
                                        symId, op.ToString());
                                    log.Error("If the chunk sizes for {op} are set very low, that may be the cause", op);
                                    return new SilentServiceException($"Too many continuationPoints for {op}", ex, op);
                                case StatusCodes.BadTooManyOperations:
                                    log.Error("Too many operations, this is most likely due to chunkSize being set too high: {code} at operation {op}",
                                        symId, op.ToString());
                                    log.Error("Try lowering the chunk sizes for {op}", op);
                                    return new SilentServiceException($"Too many operations", ex, op);
                            }
                            break;
                        case SourceOp.CloseSession:
                            log.Error("Failed to close session, this is almost always due to the session already being closed: {code}", symId);
                            return new SilentServiceException("Failed to close session", ex, op);
                    }
                    log.Error(ex, "Unexpected service result exception in operation {op}: {code}", op, symId);
                    return new SilentServiceException("Unexpected error", ex, op);
            }
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

        public SilentServiceException(string msg, ServiceResultException ex, ExtractorUtils.SourceOp op)
            : base($"{msg}: code {ex?.StatusCode ?? StatusCodes.BadUnexpectedError}, operation {op}", ex)
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
}
