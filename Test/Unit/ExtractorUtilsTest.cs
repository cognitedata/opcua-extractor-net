using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Serilog;
using Serilog.Events;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Xunit;

using SourceOp = Cognite.OpcUa.ExtractorUtils.SourceOp;


namespace Test.Unit
{
    public class ExtractorUtilsTest
    {
        [Fact]
        public void TestSortNodes()
        {
            // Empty
            IEnumerable<UANode> objects;
            IEnumerable<UAVariable> variables;
            (objects, variables) = ExtractorUtils.SortNodes(Enumerable.Empty<UANode>());
            Assert.Empty(objects);
            Assert.Empty(variables);

            // Null
            Assert.Throws<ArgumentNullException>(() => ExtractorUtils.SortNodes(null));

            var arrParent = new UAVariable(new NodeId("arr1"), "arr1", NodeId.Null);
            arrParent.VariableAttributes.ValueRank = 1;
            arrParent.VariableAttributes.ArrayDimensions = new [] { 2 };
            var children = arrParent.CreateArrayChildren();
            // Populated
            var nodes = new UANode[]
            {
                new UANode(new NodeId("object1"), "obj1", NodeId.Null, NodeClass.Object),
                new UANode(new NodeId("object2"), "obj2", NodeId.Null, NodeClass.Object),
                new UAVariable(new NodeId("var1"), "var1", NodeId.Null),
                new UAVariable(new NodeId("var2"), "var2", NodeId.Null),
                arrParent,
                new UAVariable(new NodeId("arr2"), "arr2", NodeId.Null)
            };
            (nodes[5].Attributes as Cognite.OpcUa.Types.VariableAttributes).ValueRank = 1;
            (nodes[5].Attributes as Cognite.OpcUa.Types.VariableAttributes).ArrayDimensions = new [] { 2 };

            (objects, variables) = ExtractorUtils.SortNodes(nodes.Concat(children));
            Assert.Equal(4, objects.Count());
            Assert.Equal(4, variables.Count());

            Assert.Equal(8, objects.Concat(variables).DistinctBy(node => (node.Id, node is UAVariable variable ? variable.Index : -1)).Count());
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
        class DummyLogger : ILogger
        {
            public List<LogEvent> Events { get; } = new List<LogEvent>();
            private object mutex = new object();
            public void Write(LogEvent logEvent)
            {
                lock (mutex)
                {
                    Events.Add(logEvent);
                }
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
                ex = new SilentServiceException("msg", new ServiceResultException(), ExtractorUtils.SourceOp.Browse);
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
            Assert.Equal(dbgC, logger.Events.Count(evt => evt.Level == LogEventLevel.Debug));
            Assert.Equal(errC, logger.Events.Count(evt => evt.Level == LogEventLevel.Error));
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
        [InlineData(StatusCodes.BadNotConnected, SourceOp.Browse, 0, false)]
        [InlineData(StatusCodes.BadIdentityTokenInvalid, SourceOp.CreateSession, 2, true)]
        [InlineData(StatusCodes.BadIdentityTokenRejected, SourceOp.CreateSession, 1, true)]
        [InlineData(StatusCodes.BadCertificateUntrusted, SourceOp.CreateSession, 2, true)]
        [InlineData(StatusCodes.BadCertificateUntrusted, SourceOp.Browse, 0, false)]
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
        public void TestHandleServiceException(uint code, SourceOp sourceOp, int errC, bool isSilent)
        {
            var logger = new DummyLogger();
            var serviceException = new ServiceResultException(code);
            var exc = ExtractorUtils.HandleServiceResult(logger, serviceException, sourceOp);
            Assert.Equal(errC, logger.Events.Count(evt => evt.Level == LogEventLevel.Error));
            Assert.Equal(errC, logger.Events.Count);
            if (isSilent)
            {
                Assert.IsType<SilentServiceException>(exc);
            }
        }
    }
}
