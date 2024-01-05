using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Cognite.OpcUa.Nodes;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using SourceOp = Cognite.OpcUa.ExtractorUtils.SourceOp;


namespace Test.Unit
{
    public class ExtractorUtilsTest
    {
        public ExtractorUtilsTest()
        {
        }

        [Fact]
        public void TestSortNodes()
        {
            // Empty
            IEnumerable<BaseUANode> objects;
            IEnumerable<UAVariable> variables;
            (objects, variables) = ExtractorUtils.SortNodes(Enumerable.Empty<BaseUANode>());
            Assert.Empty(objects);
            Assert.Empty(variables);

            // Null
            Assert.Throws<ArgumentNullException>(() => ExtractorUtils.SortNodes(null));

            var arrParent = new UAVariable(new NodeId("arr1", 0), "arr1", null, null, NodeId.Null, null);
            arrParent.FullAttributes.ValueRank = 1;
            arrParent.FullAttributes.ArrayDimensions = new[] { 2 };
            var children = arrParent.CreateTimeseries();
            // Populated
            var nodes = new BaseUANode[]
            {
                new UAObject(new NodeId("object1", 0), "obj1", null, null, NodeId.Null, null),
                new UAObject(new NodeId("object2", 0), "obj2", null, null, NodeId.Null, null),
                new UAVariable(new NodeId("var1", 0), "var1", null, null, NodeId.Null, null),
                new UAVariable(new NodeId("var2", 0), "var2", null, null, NodeId.Null, null),
                arrParent,
                new UAVariable(new NodeId("arr2", 0), "arr2", null, null, NodeId.Null, null)
            };
            (nodes[5].Attributes as Cognite.OpcUa.Nodes.VariableAttributes).ValueRank = 1;
            (nodes[5].Attributes as Cognite.OpcUa.Nodes.VariableAttributes).ArrayDimensions = new[] { 2 };

            (objects, variables) = ExtractorUtils.SortNodes(nodes.Concat(children));
            Assert.Equal(4, objects.Count());
            Assert.Equal(4, variables.Count());

            Assert.Equal(8, objects.Concat(variables).DistinctBy(node => (node.Id, node is UAVariableMember variable ? variable.Index : -1)).Count());
        }
        [Fact]
        public void TestGetRootException()
        {
            var root = new ExtractorFailureException("some exception");
            // 1 layer
            var aex1 = new AggregateException(root);
            Assert.Equal(root, ExtractorUtils.GetRootExceptionOfType<ExtractorFailureException>(aex1));

            // 3 layer
            var aex2 = new AggregateException(new AggregateException(aex1));
            Assert.Equal(root, ExtractorUtils.GetRootExceptionOfType<ExtractorFailureException>(aex2));
        }

        private sealed class LogEvent
        {
            public LogLevel LogLevel { get; set; }
            public EventId EventId { get; set; }
            public Exception Exception { get; set; }
        }

        private sealed class DummyLogger : ILogger
        {
            public List<LogEvent> Events { get; } = new List<LogEvent>();
            private readonly object mutex = new object();

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
                Exception exception, Func<TState, Exception, string> formatter)
            {
                lock (mutex)
                {
                    Events.Add(new LogEvent
                    {
                        LogLevel = logLevel,
                        EventId = eventId,
                        Exception = exception
                    });
                }
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return true;
            }

            public IDisposable BeginScope<TState>(TState state)
            {
                throw new NotImplementedException();
            }
        }

        [Theory]
        [InlineData(typeof(Exception), 0, 1, false)]
        [InlineData(typeof(Exception), 0, 1, true)]
        [InlineData(typeof(AggregateException), 0, 1, false)]
        [InlineData(typeof(SilentServiceException), 1, 0, false)]
        [InlineData(typeof(SilentServiceException), 1, 0, true)]
        [InlineData(typeof(ExtractorFailureException), 1, 1, false)]
        [InlineData(typeof(ExtractorFailureException), 1, 1, true)]
        public void TestLogException(Type type, int dbgC, int errC, bool nested)
        {
            Exception ex;
            if (type == typeof(SilentServiceException))
            {
                ex = new SilentServiceException("msg", new ServiceResultException(), SourceOp.Browse);
            }
            else
            {
                ex = (Exception)Activator.CreateInstance(type);
            }

            if (nested)
            {
                ex = new AggregateException(ex);
            }

            var logger = new DummyLogger();
            ExtractorUtils.LogException(logger, ex, "message", "silentMessage");
            Assert.Equal(dbgC, logger.Events.Count(evt => evt.LogLevel == LogLevel.Debug));
            Assert.Equal(errC, logger.Events.Count(evt => evt.LogLevel == LogLevel.Error));
            Assert.Equal(dbgC + errC, logger.Events.Count);
        }
        [Theory]
        [InlineData(StatusCodes.BadDecodingError, SourceOp.Browse, 3, true)]
        [InlineData(StatusCodes.BadUnknownResponse, SourceOp.Browse, 3, true)]
        [InlineData(StatusCodes.BadCertificateChainIncomplete, SourceOp.Browse, 1, true)]
        [InlineData(StatusCodes.BadNothingToDo, SourceOp.Browse, 1, true)]
        [InlineData(StatusCodes.BadSessionClosed, SourceOp.Browse, 1, true)]
        [InlineData(StatusCodes.BadServerNotConnected, SourceOp.Browse, 2, true)]
        [InlineData(StatusCodes.BadServerHalted, SourceOp.Browse, 1, true)]
        [InlineData(StatusCodes.BadNotConnected, SourceOp.SelectEndpoint, 2, true)]
        [InlineData(StatusCodes.BadNotConnected, SourceOp.Browse, 1, false)]
        [InlineData(StatusCodes.BadIdentityTokenInvalid, SourceOp.CreateSession, 2, true)]
        [InlineData(StatusCodes.BadIdentityTokenRejected, SourceOp.CreateSession, 1, true)]
        [InlineData(StatusCodes.BadCertificateUntrusted, SourceOp.CreateSession, 2, true)]
        [InlineData(StatusCodes.BadCertificateUntrusted, SourceOp.Browse, 1, false)]
        [InlineData(StatusCodes.BadNodeIdInvalid, SourceOp.ReadRootNode, 1, true)]
        [InlineData(StatusCodes.BadNodeIdInvalid, SourceOp.Browse, 1, true)]
        [InlineData(StatusCodes.BadNodeIdInvalid, SourceOp.ReadAttributes, 1, true)]
        [InlineData(StatusCodes.BadUserAccessDenied, SourceOp.ReadAttributes, 1, true)]
        [InlineData(StatusCodes.BadSecurityModeInsufficient, SourceOp.ReadAttributes, 2, true)]
        [InlineData(StatusCodes.BadTooManySubscriptions, SourceOp.CreateSubscription, 3, true)]
        [InlineData(StatusCodes.BadServiceUnsupported, SourceOp.CreateSubscription, 2, true)]
        [InlineData(StatusCodes.BadSubscriptionIdInvalid, SourceOp.CreateSubscription, 3, true)]
        [InlineData(StatusCodes.BadSubscriptionIdInvalid, SourceOp.CreateMonitoredItems, 3, true)]
        [InlineData(StatusCodes.BadFilterNotAllowed, SourceOp.CreateMonitoredItems, 2, true)]
        [InlineData(StatusCodes.BadTooManyMonitoredItems, SourceOp.CreateMonitoredItems, 3, true)]
        [InlineData(StatusCodes.BadNodeIdInvalid, SourceOp.HistoryRead, 1, true)]
        [InlineData(StatusCodes.BadUserAccessDenied, SourceOp.HistoryRead, 1, true)]
        [InlineData(StatusCodes.BadTooManyOperations, SourceOp.HistoryRead, 2, true)]
        [InlineData(StatusCodes.BadHistoryOperationUnsupported, SourceOp.HistoryRead, 3, true)]
        [InlineData(StatusCodes.BadFilterNotAllowed, SourceOp.HistoryReadEvents, 2, true)]
        [InlineData(StatusCodes.BadServiceUnsupported, SourceOp.CreateMonitoredItems, 2, true)]
        [InlineData(StatusCodes.BadNoContinuationPoints, SourceOp.CreateMonitoredItems, 2, true)]
        [InlineData(StatusCodes.BadTooManyOperations, SourceOp.CreateMonitoredItems, 2, true)]
        [InlineData(StatusCodes.BadTooManyOperations, SourceOp.CloseSession, 1, true)]
        [InlineData(StatusCodes.BadRequestInterrupted, SourceOp.CreateSubscription, 1, true)]
        public void TestHandleServiceException(uint code, SourceOp sourceOp, int errC, bool isHandled)
        {
            var logger = new DummyLogger();
            var serviceException = new ServiceResultException(code);
            var exc = ExtractorUtils.HandleServiceResult(logger, serviceException, sourceOp);
            Assert.Equal(errC, logger.Events.Count(evt => evt.LogLevel == LogLevel.Error));
            Assert.Equal(errC, logger.Events.Count);
            var serviceEx = Assert.IsType<SilentServiceException>(exc);
            if (isHandled) Assert.NotNull(serviceEx.InnerServiceException);

            exc = ExtractorUtils.HandleServiceResult(logger, new AggregateException(serviceException), sourceOp);
            Assert.Equal(errC * 2, logger.Events.Count(evt => evt.LogLevel == LogLevel.Error));
            Assert.Equal(errC * 2, logger.Events.Count);
            serviceEx = Assert.IsType<SilentServiceException>(exc);
            if (isHandled) Assert.NotNull(serviceEx.InnerServiceException);
        }

        [Fact]
        public void TestHandleNestedException()
        {
            var logger = new DummyLogger();
            var inner = new ServiceResultException(StatusCodes.BadRequestTooLarge);
            var exception = new ServiceResultException(StatusCodes.BadRequestInterrupted, inner);

            var exc = ExtractorUtils.HandleServiceResult(logger, exception, SourceOp.CreateSubscription);
            Assert.Equal(3, logger.Events.Count);

            var serviceEx = Assert.IsType<SilentServiceException>(exc);
            Assert.NotNull(serviceEx.InnerServiceException);
        }
    }
}
