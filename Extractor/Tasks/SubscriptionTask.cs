using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Utils.Unstable.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Subscriptions;

namespace Cognite.OpcUa.Tasks
{
    public class SubscriptionTask : BaseSchedulableTask
    {
        private UAExtractor extractor;
        private UAClient client;
        private FullConfig config;

        private List<VariableExtractionState> pendingVariables = new List<VariableExtractionState>();
        private List<EventExtractionState> pendingEmitters = new List<EventExtractionState>();

        private object stateLock = new object();

        public void AddVariables(IEnumerable<VariableExtractionState> variables)
        {
            lock (stateLock)
            {
                pendingVariables.AddRange(variables.Where(v => v.ShouldSubscribe));
            }
        }

        public void AddEmitters(IEnumerable<EventExtractionState> emitters)
        {
            lock (stateLock)
            {
                pendingEmitters.AddRange(emitters.Where(e => e.ShouldSubscribe));
            }
        }

        public SubscriptionTask(UAExtractor extractor, UAClient client, FullConfig config)
        {
            this.extractor = extractor;
            this.client = client;
            this.config = config;
        }

        public override bool ErrorIsFatal => true;

        public override string Name => "Create Subscriptions";

        public override TaskMetadata Metadata => new TaskMetadata(CogniteSdk.Alpha.TaskType.batch);

        public override bool CanRunNow()
        {
            // We are always ready, technically, if we are connected.
            // We rely on the extractor to re-schedule the subscription task when needed.
            return client.SessionManager.Session?.Connected == true;
        }

        private async Task SubscribeToDataPoints(IEnumerable<VariableExtractionState> variables, CancellationToken token)
        {
            if (!config.Subscriptions.DataPoints || !variables.Any()) return;

            if (client.SubscriptionManager == null) throw new InvalidOperationException("Client not initialized");

            await client.SubscriptionManager.EnqueueTaskAndWait(new DataPointSubscriptionTask(
                extractor.Streamer.DataSubscriptionHandler,
                variables,
                extractor
            ), token);
        }

        private async Task SubscribeToEvents(IEnumerable<EventExtractionState> emitters, CancellationToken token)
        {
            if (!config.Subscriptions.Events || !emitters.Any()) return;

            if (client.SubscriptionManager == null) throw new InvalidOperationException("Client not initialized");

            await client.SubscriptionManager.EnqueueTaskAndWait(new EventSubscriptionTask(
                extractor.Streamer.EventSubscriptionHandler,
                emitters,
                client.BuildEventFilter(extractor.TypeManager.EventFields),
                extractor
            ), token);
        }

        private async Task SubscribeToAuditEvents(CancellationToken token)
        {
            if (!config.Extraction.EnableAuditDiscovery) return;

            if (client.SubscriptionManager == null) throw new InvalidOperationException("Client not initialized");

            await client.SubscriptionManager.EnqueueTaskAndWait(new AuditSubscriptionTask(
                extractor.AuditEventSubscriptionHandler,
                extractor
            ), token);
        }

        private string GetMessage(List<VariableExtractionState> variables, List<EventExtractionState> emitters)
        {
            if (variables.Count == 0 && emitters.Count == 0 && !config.Extraction.EnableAuditDiscovery)
            {
                return "Created no new monitored items";
            }

            var message = new StringBuilder("Created monitored items for: ");
            bool needsComma = false;
            if (variables.Count > 0)
            {
                message.AppendFormat("{0} variables", variables.Count);
                needsComma = true;
            }
            if (emitters.Count > 0)
            {
                if (needsComma) message.Append(", ");
                message.AppendFormat("{0} event emitters", emitters.Count);
            }
            if (config.Extraction.EnableAuditDiscovery)
            {
                if (needsComma) message.Append(", ");
                message.Append("audit events");
            }

            return message.ToString();
        }

        public override async Task<TaskUpdatePayload?> Run(BaseErrorReporter task, CancellationToken token)
        {
            var variables = new List<VariableExtractionState>(pendingVariables.Count);
            var emitters = new List<EventExtractionState>(pendingEmitters.Count);

            lock (stateLock)
            {
                variables.AddRange(pendingVariables);
                emitters.AddRange(pendingEmitters);
                pendingVariables.Clear();
                pendingEmitters.Clear();
            }

            var tasks = new Task[]
            {
                SubscribeToDataPoints(variables, token),
                SubscribeToEvents(emitters, token),
                SubscribeToAuditEvents(token)
            };

            await Task.WhenAll(tasks);

            return new TaskUpdatePayload(GetMessage(variables, emitters));
        }
    }
}