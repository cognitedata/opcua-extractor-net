using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils.Unstable.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Utils;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Tasks
{
    public class HistoryTask : BaseSchedulableTask
    {
        public override bool ErrorIsFatal => true;
        public override string Name => "Read historical data from OPC-UA server";
        public override TaskMetadata Metadata => new TaskMetadata(CogniteSdk.Alpha.TaskType.batch);

        private readonly BlockingResourceCounter continuationPoints;
        private readonly TypeManager typeManager;
        private readonly UAClient client;
        private readonly UAExtractor extractor;
        private readonly FullConfig config;

        private readonly Dictionary<NodeId, VariableExtractionState> activeVarStates = new();
        private readonly Dictionary<NodeId, EventExtractionState> activeEventStates = new();
        private readonly object statesLock = new();
        private readonly ILogger<HistoryTask> log;
        private readonly TaskThrottler throttler;
        private readonly State state;

        /// <summary>
        /// This flag is set when the history task state goes bad, and
        /// is cleared when we start a new history run with good state.
        /// This helps prevent race conditions like
        ///  - State is good, history starts
        ///  - State goes bad
        ///  - Before history has been able to shut down fully, state goes good again.
        ///  - Now live data was ingested that wasn't actually ready due to unfinished history.
        /// </summary>
        public bool CurrentHistoryRunIsBad { get; private set; }

        public override ITimeSpanProvider? Schedule { get; }

        private Action readyCallback = () => { };

        public override void RegisterReadyCallback(Action callback)
        {
            readyCallback = callback;
        }

        public HistoryTask(
            ILogger<HistoryTask> log,
            UAClient client,
            UAExtractor extractor,
            TypeManager typeManager,
            FullConfig config
        )
        {
            this.log = log;
            this.client = client;
            this.extractor = extractor;
            this.typeManager = typeManager;
            this.config = config;
            var throttling = config.History.Throttling;
            throttler = new TaskThrottler(throttling.MaxParallelism, false, throttling.MaxPerMinute, TimeSpan.FromMinutes(1));
            continuationPoints = new BlockingResourceCounter(throttling.MaxNodeParallelism > 0 ? throttling.MaxNodeParallelism : 1_000);
            if (extractor.StateStorage == null)
            {
                throw new InvalidOperationException("History task requires state storage to be configured");
            }
            state = new State(config);
            Schedule = config.History.RestartPeriodValue;
        }

        public override bool CanRunNow()
        {
            return state.IsGood;
        }

        public override async Task<TaskUpdatePayload?> Run(BaseErrorReporter task, CancellationToken token)
        {
            RestartHistoryInStates();
            CurrentHistoryRunIsBad = !state.IsGood;
            await RunAllHistory(token);
            // TODO: Collect some useful information here to put in the message, perhaps
            // aggregated information about how much history was read, for how many states.
            return new TaskUpdatePayload();
        }

        public enum StateIssue
        {
            ServiceLevel,
            ServerConnection,
            DataPointSubscription,
            EventSubscription,
            NodeHierarchyRead,
            DataPushFailing,
            EventsPushFailing,
            StateRestorationPending,
        }

        class State
        {
            private readonly HashSet<StateIssue> issues = new();

            public bool AddIssue(StateIssue issue)
            {
                return issues.Add(issue);
            }

            public bool RemoveIssue(StateIssue issue)
            {
                return issues.Remove(issue);
            }

            public State(FullConfig config)
            {
                if (config.Subscriptions.Events && config.Events.Enabled && config.Events.History)
                {
                    issues.Add(StateIssue.EventSubscription);
                }
                if (config.Subscriptions.DataPoints && config.History.Data)
                {
                    issues.Add(StateIssue.DataPointSubscription);
                }
                issues.Add(StateIssue.StateRestorationPending);
                issues.Add(StateIssue.NodeHierarchyRead);
            }

            public bool IsGood => issues.Count == 0;

            public override string ToString()
            {
                if (IsGood)
                {
                    return "Good";
                }

                return "Bad. Issues: " + string.Join(", ", issues);
            }
        }

        private void RestartHistoryInStates()
        {
            lock (statesLock)
            {
                foreach (var state in activeVarStates)
                {
                    state.Value.RestartHistory();
                }
                foreach (var state in activeEventStates)
                {
                    state.Value.RestartHistory();
                }
            }
        }

        private async Task RunAllHistory(CancellationToken token)
        {
            IEnumerable<EventExtractionState> eventStates;
            IEnumerable<VariableExtractionState> variableStates;

            lock (statesLock)
            {
                eventStates = activeEventStates.Values.ToArray();
                variableStates = activeVarStates.Values.ToArray();
            }

            var tasks = new List<Task>();

            if (config.History.Data && variableStates.Any())
            {
                tasks.Add(Task.Run(async () =>
                {
                    await RunHistoryBatch(variableStates, HistoryReadType.FrontfillData, token);
                    if (config.History.Backfill)
                    {
                        await RunHistoryBatch(variableStates, HistoryReadType.BackfillData, token);
                    }
                }));
            }
            if (config.Events.History && eventStates.Any())
            {
                tasks.Add(Task.Run(async () =>
                {
                    await RunHistoryBatch(eventStates, HistoryReadType.FrontfillEvents, token);
                    if (config.History.Backfill)
                    {
                        await RunHistoryBatch(eventStates, HistoryReadType.BackfillEvents, token);
                    }
                }));
            }
            await Task.WhenAll(tasks);
        }

        private async Task RunHistoryBatch(IEnumerable<UAHistoryExtractionState> states, HistoryReadType type, CancellationToken token)
        {
            using var scheduler = new HistoryScheduler(log, client, extractor, typeManager, config, type,
                throttler, continuationPoints, states, token);

            try
            {
                await scheduler.RunAsync();
            }
            catch (AggregateException aex)
            {
                throw new SmartAggregateException(aex);
            }
        }

        public void RemoveIssue(StateIssue issue)
        {
            lock (statesLock)
            {
                bool stateOk = state.IsGood;

                if (state.RemoveIssue(issue))
                {
                    log.LogDebug("Removed history issue: {Issue}. New state: {State}", issue, state);
                }

                if (stateOk != state.IsGood)
                {
                    log.LogDebug("History state changed to good, history will restart");
                    readyCallback();
                }
            }
        }

        public void AddIssue(StateIssue issue)
        {
            lock (statesLock)
            {
                bool stateOk = state.IsGood;

                if (state.AddIssue(issue))
                {
                    log.LogDebug("Added history issue: {Issue}. New state: {State}", issue, state);
                    CurrentHistoryRunIsBad = true;
                }

                if (stateOk != state.IsGood)
                {
                    log.LogDebug("History state changed to bad, history will stop");
                    extractor.RestartHistory();
                }
            }
        }

        public void AddStates(IEnumerable<VariableExtractionState> varStates, IEnumerable<EventExtractionState> eventStates)
        {
            lock (statesLock)
            {
                bool anyAdded = false;
                foreach (var state in varStates.Where(s => s.FrontfillEnabled))
                {
                    anyAdded |= activeVarStates.TryAdd(state.SourceId, state);
                }

                foreach (var state in eventStates.Where(s => s.FrontfillEnabled))
                {
                    anyAdded |= activeEventStates.TryAdd(state.SourceId, state);
                }
                if (state.IsGood && anyAdded)
                {
                    extractor.RestartHistory(); // Cancel any currently running history, to make sure the new states are included in the next run.
                }
            }
        }

        public void MaxNodeParallelismChanged()
        {
            continuationPoints.SetCapacity(config.History.Throttling.MaxNodeParallelism > 0 ? config.History.Throttling.MaxNodeParallelism : 1_000);
        }
    }
}
