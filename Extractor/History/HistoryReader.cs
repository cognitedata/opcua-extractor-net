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

using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.CommandLine.Parsing;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.History
{
    public sealed class HistoryReader : IDisposable
    {
        private readonly UAClient uaClient;
        private readonly UAExtractor extractor;
        private readonly FullConfig config;
        private readonly TypeManager typeManager;
        private CancellationTokenSource source;
        private CancellationTokenSource? runTaskSource;
        // private ILogger log = Log.Logger.ForContext<HistoryReader>();
        private readonly TaskThrottler throttler;
        private readonly BlockingResourceCounter continuationPoints;
        private readonly OperationWaiter waiter;
        private readonly ILogger<HistoryReader> log;

        private readonly Dictionary<NodeId, VariableExtractionState> activeVarStates = new();
        private readonly Dictionary<NodeId, EventExtractionState> activeEventStates = new();
        private readonly object statesLock = new();

        public bool IsRunning => runTaskSource != null;

        public HistoryReader(ILogger<HistoryReader> log,
            UAClient uaClient, UAExtractor extractor, TypeManager typeManager, FullConfig config, CancellationToken token)
        {
            this.log = log;
            this.config = config;
            this.uaClient = uaClient;
            this.extractor = extractor;
            this.typeManager = typeManager;
            var throttling = config.History.Throttling;
            throttler = new TaskThrottler(throttling.MaxParallelism, false, throttling.MaxPerMinute, TimeSpan.FromMinutes(1));
            source = CancellationTokenSource.CreateLinkedTokenSource(token);
            continuationPoints = new BlockingResourceCounter(
                throttling.MaxNodeParallelism > 0 ? throttling.MaxNodeParallelism : 1_000);
            waiter = new OperationWaiter();
            state = new State(config);
        }

        private async Task RunHistoryBatch(IEnumerable<UAHistoryExtractionState> states, HistoryReadType type)
        {
            using var scheduler = new HistoryScheduler(log, uaClient, extractor, typeManager, config, type,
                throttler, continuationPoints, states, source.Token);

            using var op = waiter.GetInstance();

            try
            {
                await scheduler.RunAsync();
            }
            catch (AggregateException aex)
            {
                throw new SmartAggregateException(aex);
            }
        }

        public void MaxNodeParallelismChanged()
        {
            continuationPoints.SetCapacity(config.History.Throttling.MaxNodeParallelism > 0 ? config.History.Throttling.MaxNodeParallelism : 1_000);
        }

        private readonly ManualResetEvent stateChangedEvent = new ManualResetEvent(true);

        private Task? activeTask;

        public async Task Run(CancellationToken token)
        {
            if (runTaskSource != null) throw new InvalidOperationException("Attempt to start history reader more than once");
            runTaskSource = CancellationTokenSource.CreateLinkedTokenSource(token);
            var waitTask = CommonUtils.WaitAsync(stateChangedEvent, Timeout.InfiniteTimeSpan, runTaskSource.Token);
            while (!runTaskSource.Token.IsCancellationRequested)
            {
                if (activeTask != null)
                {
                    log.LogDebug("History is running, waiting for waker");
                    var res = await Task.WhenAny(activeTask, waitTask);

                    if (runTaskSource.IsCancellationRequested) break;

                    if (res == activeTask)
                    {
                        if (res.Exception != null && !typeof(OperationCanceledException).IsAssignableFrom(res.Exception.GetType()))
                        {
                            ExceptionDispatchInfo.Capture(res.Exception).Throw();
                        }

                        activeTask = null;
                        continue;
                    }
                }
                else
                {
                    await waitTask;
                    if (runTaskSource.IsCancellationRequested) break;
                }

                stateChangedEvent.Reset();
                waitTask = CommonUtils.WaitAsync(stateChangedEvent, Timeout.InfiniteTimeSpan, runTaskSource.Token);

                if (activeTask != null)
                {
                    log.LogInformation("Received event, stopping history");

                    await Terminate(runTaskSource.Token);

                    var res = await Task.WhenAny(activeTask, Task.Delay(5, runTaskSource.Token));
                    if (res != activeTask)
                    {
                        log.LogWarning("Timed out waiting for active history task to complete");
                    }
                    else if (res.Exception != null && !typeof(OperationCanceledException).IsAssignableFrom(res.Exception.GetType()))
                    {
                        ExceptionDispatchInfo.Capture(res.Exception).Throw();
                    }
                }

                RestartHistoryInStates();

                if (state.IsGood)
                {
                    log.LogInformation("State is good: {State}, starting history read", state);

                    activeTask = RunAllHistory();
                }
                else
                {
                    log.LogInformation("State is bad: {State}, starting history once status improves", state);
                }
            }
            await Terminate(token);
            runTaskSource = null;
        }

        private async Task RunAllHistory()
        {
            IEnumerable<EventExtractionState> eventStates;
            IEnumerable<VariableExtractionState> variableStates;

            lock (statesLock)
            {
                eventStates = activeEventStates.Values.ToArray();
                variableStates = activeVarStates.Values.ToArray();
            }

            var tasks = new List<Task>();

            if (!config.History.Enabled) return;

            if (config.History.Data && variableStates.Any())
            {
                tasks.Add(Task.Run(async () =>
                {
                    await RunHistoryBatch(variableStates, HistoryReadType.FrontfillData);
                    if (config.History.Backfill)
                    {
                        await RunHistoryBatch(variableStates, HistoryReadType.BackfillData);
                    }
                }));
            }
            if (config.Events.History && eventStates.Any())
            {
                tasks.Add(Task.Run(async () =>
                {
                    await RunHistoryBatch(eventStates, HistoryReadType.FrontfillEvents);
                    if (config.History.Backfill)
                    {
                        await RunHistoryBatch(eventStates, HistoryReadType.BackfillEvents);
                    }
                }));
            }
            await Task.WhenAll(tasks);
        }

        private readonly SemaphoreSlim terminateSem = new SemaphoreSlim(1);

        class State
        {
            private HashSet<StateIssue> issues = new();

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

        private readonly State state;

        public enum StateIssue
        {
            ServiceLevel,
            ServerConnection,
            DataPointSubscription,
            EventSubscription,
            NodeHierarchyRead
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
                    stateChangedEvent.Set();
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
                }

                if (stateOk != state.IsGood)
                {
                    log.LogDebug("History state changed to bad, history will stop");
                    stateChangedEvent.Set();
                }
            }
        }

        public void AddStates(IEnumerable<VariableExtractionState> varStates, IEnumerable<EventExtractionState> eventStates)
        {
            lock (statesLock)
            {
                foreach (var state in varStates.Where(s => s.FrontfillEnabled))
                {
                    activeVarStates[state.SourceId] = state;
                }

                foreach (var state in eventStates.Where(s => s.FrontfillEnabled))
                {
                    activeEventStates[state.SourceId] = state;
                }
                if (state.IsGood)
                {
                    stateChangedEvent.Set();
                }
            }
        }

        public void RequestRestart()
        {
            stateChangedEvent.Set();
        }

        public async Task RequestRestartWaitForTermination(CancellationToken token)
        {
            await Terminate(token);
            RestartHistoryInStates();
            stateChangedEvent.Set();
        }

        /// <summary>
        /// Request the history read terminate, then wait for all operations to finish before quitting.
        /// </summary>
        /// <param name="timeoutsec">Timeout in seconds</param>
        /// <returns>True if successfully aborted, false if waiting timed out</returns>
        private async Task<bool> Terminate(CancellationToken token, int timeoutsec = 30)
        {
            await terminateSem.WaitAsync(token);
            try
            {
                source.Cancel();
                log.LogInformation("Terminating any active history reads");
                bool timedOut = !await waiter.Wait(timeoutsec * 1000, token);
                if (timedOut)
                {
                    log.LogWarning("Terminating history reads timed out after {Timeout} seconds", timeoutsec);
                }
                else
                {
                    log.LogInformation("History reads successfully terminated");
                }
                source.Dispose();
                source = CancellationTokenSource.CreateLinkedTokenSource(token);
                return timedOut;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Unexpected error terminating history: {Err}", ex.Message);
                return false;
            }
            finally
            {
                terminateSem.Release();
            }
        }

        public void Dispose()
        {
            source.Cancel();
            source.Dispose();
            runTaskSource?.Cancel();
            runTaskSource?.Dispose();
            waiter.Dispose();
            throttler.Dispose();
        }
    }
}
