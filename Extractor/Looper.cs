/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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

using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Looper used to manage loops in the extractor.
    /// </summary>
    public sealed class Looper : IDisposable
    {
        private readonly UAExtractor extractor;
        private readonly FullConfig config;

        private bool nextPushFlag;
        private bool restart;

        private readonly List<Task> tasks = new List<Task>();
        private readonly IEnumerable<IPusher> pushers;
        private readonly ILogger log = Log.Logger.ForContext(typeof(Looper));
        private readonly ManualResetEvent triggerUpdateOperations = new ManualResetEvent(false);
        private readonly ManualResetEvent triggerHistoryRestart = new ManualResetEvent(false);
        private readonly ManualResetEvent triggerGrowTaskList = new ManualResetEvent(false);
        private readonly ManualResetEvent triggerPush = new ManualResetEvent(false);
        private readonly ManualResetEvent triggerStoreState = new ManualResetEvent(false);
        private readonly ManualResetEvent triggerRebrowse = new ManualResetEvent(false);

        public Looper(UAExtractor extractor, FullConfig config, IEnumerable<IPusher> pushers)
        {
            this.extractor = extractor;
            this.config = config;
            this.pushers = pushers;
        }
        /// <summary>
        /// Wait for the next push of data to CDF
        /// </summary>
        /// <param name="timeout">Timeout in 1/10th of a second</param>
        public async Task WaitForNextPush(bool trigger = false, int timeout = 100)
        {
            nextPushFlag = false;
            if (trigger)
            {
                TriggerPush();
            }
            int time = 0;
            while (!nextPushFlag && time++ < timeout) await Task.Delay(100);
            if (time >= timeout && !nextPushFlag)
            {
                throw new TimeoutException("Waiting for push timed out");
            }
            log.Debug("Waited {s} milliseconds for push", time * 100);
        }
        /// <summary>
        /// Trigger a push immediately, if one is not currently happening
        /// </summary>
        public void TriggerPush()
        {
            triggerPush.Set();
        }
        /// <summary>
        /// Schedule a restart of the extractor.
        /// </summary>
        public void Restart()
        {
            restart = true;
            triggerUpdateOperations.Set();
        }
        /// <summary>
        /// Schedule update in the update loop.
        /// </summary>
        public void ScheduleUpdate()
        {
            triggerUpdateOperations.Set();
        }
        /// <summary>
        /// Schedule a list of tasks in the main task loop.
        /// </summary>
        public void ScheduleTasks(IEnumerable<Task> newTasks)
        {
            tasks.AddRange(newTasks);
            triggerGrowTaskList.Set();
        }
        public void TriggerHistoryRestart()
        {
            triggerHistoryRestart.Set();
        }
        public void TriggerRebrowse()
        {
            triggerRebrowse.Set();
        }
        public void TriggerStoreState()
        {
            triggerStoreState.Set();
        }
        /// <summary>
        /// Main task loop, terminates on any task failure or if all tasks are finished.
        /// </summary>
        /// <param name="synchTasks">Initial history tasks</param>
        public async Task InitTaskLoop(IEnumerable<Task> synchTasks, CancellationToken token)
        {
            tasks.Clear();
            tasks.Add(Task.Run(() => PushersLoop(token), token));
            tasks.Add(Task.Run(() => ExtraTaskLoop(token), token));
            tasks.AddRange(synchTasks);

            if (config.Extraction.AutoRebrowsePeriod > 0)
            {
                tasks.Add(Task.Run(() => RebrowseLoop(token), CancellationToken.None));
            }

            if (extractor.StateStorage != null && config.StateStorage.Interval > 0)
            {
                tasks.Add(Task.Run(() => StoreStateLoop(token), CancellationToken.None));
            }
            tasks.Add(Task.Run(() => HistoryRestartLoop(token), CancellationToken.None));

            tasks.Add(SafeWait(triggerGrowTaskList, Timeout.InfiniteTimeSpan, token));

            Task failedTask = null;

            while (tasks.Any())
            {
                try
                {
                    var terminated = await Task.WhenAny(tasks);
                    if (terminated.IsFaulted)
                    {
                        ExtractorUtils.LogException(log, terminated.Exception,
                            "Unexpected error in main task list",
                            "Handled error in main task list");
                    }
                }
                catch (Exception ex)
                {
                    ExtractorUtils.LogException(log, ex, "Unexpected error in main task list", "Handled error in main task list");
                }
                failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);

                if (failedTask != null) break;

                var toRemove = tasks.Where(task => task.IsCompleted).ToList();
                foreach (var task in toRemove)
                {
                    tasks.Remove(task);
                }

                if (triggerGrowTaskList.WaitOne(0))
                {
                    triggerGrowTaskList.Reset();
                    tasks.Add(SafeWait(triggerGrowTaskList, Timeout.InfiniteTimeSpan, token));
                }
            }

            if (token.IsCancellationRequested) throw new TaskCanceledException();
            ExceptionDispatchInfo.Capture(failedTask.Exception).Throw();
        }
        /// <summary>
        /// Wait until the manual event is triggered, the token is canceled, or a timeout has occured.
        /// </summary>
        /// <param name="manual">Manual event</param>
        /// <param name="delay">Maximum time to wait</param>
        /// <param name="token">CancellationToken</param>
        /// <returns>Task that terminates when the delay has passed, the event has triggered, or the token is cancelled.</returns>
        private static Task SafeWait(EventWaitHandle manual, TimeSpan delay, CancellationToken token)
        {
            return Task.Run(() => WaitHandle.WaitAny(new[] { manual, token.WaitHandle }, delay));
        }

        /// <summary>
        /// Main loop for pushing data and events to destinations.
        /// </summary>
        private async Task PushersLoop(CancellationToken token)
        {
            var failingPushers = pushers.Where(pusher => pusher.DataFailing || pusher.EventsFailing || !pusher.Initialized).ToList();
            var passingPushers = pushers.Except(failingPushers).ToList();

            while (!token.IsCancellationRequested)
            {
                if (failingPushers.Any())
                {
                    var result = await Task.WhenAll(failingPushers.Select(pusher => pusher.TestConnection(config, token)));
                    var recovered = result.Select((res, idx) => (result: res, pusher: failingPushers.ElementAt(idx)))
                        .Where(x => x.result == true).ToList();

                    if (recovered.Any())
                    {
                        log.Information("Pushers {names} recovered", string.Join(", ", recovered.Select(val => val.pusher.GetType())));
                    }


                    if (recovered.Any(pair => !pair.pusher.Initialized))
                    {
                        var tasks = new List<Task>();
                        var toInit = recovered.Select(pair => pair.pusher).Where(pusher => !pusher.Initialized);
                        foreach (var pusher in toInit)
                        {
                            var (nodes, timeseries) = ExtractorUtils.SortNodes(pusher.PendingNodes);
                            var references = pusher.PendingReferences.ToList();
                            pusher.PendingNodes.Clear();
                            pusher.PendingReferences.Clear();
                            pusher.NoInit = false;
                            tasks.Add(extractor.PushNodes(nodes, timeseries, references, pusher, true));
                        }

                        await Task.WhenAll(tasks);
                    }
                    foreach (var pair in recovered)
                    {
                        if (pair.pusher.Initialized)
                        {
                            pair.pusher.DataFailing = true;
                            pair.pusher.EventsFailing = true;
                            failingPushers.Remove(pair.pusher);
                            passingPushers.Add(pair.pusher);
                        }
                    }
                }

                var waitTask = SafeWait(triggerPush, config.Extraction.DataPushDelay < 0
                    ? Timeout.InfiniteTimeSpan : TimeSpan.FromMilliseconds(config.Extraction.DataPushDelay), token);

                var results = await Task.WhenAll(
                    Task.Run(async () =>
                        await extractor.Streamer.PushDataPoints(passingPushers, failingPushers, token), token),
                    Task.Run(async () => await extractor.Streamer.PushEvents(passingPushers, failingPushers, token), token));

                if (results.Any(res => res))
                {
                    triggerHistoryRestart.Set();
                }

                var failedPushers = passingPushers.Where(pusher =>
                    pusher.DataFailing && extractor.Streamer.AllowData
                    || pusher.EventsFailing && extractor.Streamer.AllowEvents
                    || !pusher.Initialized).ToList();
                foreach (var pusher in failedPushers)
                {
                    pusher.DataFailing = extractor.Streamer.AllowData;
                    pusher.EventsFailing = extractor.Streamer.AllowEvents;
                    failingPushers.Add(pusher);
                    passingPushers.Remove(pusher);
                }

                await waitTask;
                triggerPush.Reset();
                nextPushFlag = true;
            }
        }
        /// <summary>
        /// Store the current state to the state store.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task StoreState(CancellationToken token)
        {
            await Task.WhenAll(
                extractor.StateStorage.StoreExtractionState(extractor.State.NodeStates
                    .Where(state => state.FrontfillEnabled), config.StateStorage.VariableStore, token),
                extractor.StateStorage.StoreExtractionState(extractor.State.EmitterStates
                    .Where(state => state.FrontfillEnabled), config.StateStorage.EventStore, token)
            );
        }
        /// <summary>
        /// Loop for periodically storing extraction states to litedb.
        /// </summary>
        private async Task StoreStateLoop(CancellationToken token)
        {
            var delay = TimeSpan.FromSeconds(config.StateStorage.Interval);
            while (!token.IsCancellationRequested)
            {
                var waitTask = SafeWait(triggerStoreState, delay, token);
                await StoreState(token);
                await waitTask;
                triggerStoreState.Reset();
            }
        }
        /// <summary>
        /// Loop for periodically browsing the UA hierarchy and adding subscriptions to any new nodes.
        /// </summary>
        private async Task RebrowseLoop(CancellationToken token)
        {
            var delay = TimeSpan.FromMinutes(config.Extraction.AutoRebrowsePeriod);
            await SafeWait(triggerRebrowse, delay, token);
            while (!token.IsCancellationRequested)
            {
                var waitTask = SafeWait(triggerRebrowse, delay, token);
                await extractor.Rebrowse();
                await waitTask;
                triggerRebrowse.Reset();
            }
        }
        /// <summary>
        /// Waits for triggerUpdateOperations to fire, then executes all the tasks in the queue.
        /// </summary>
        private async Task ExtraTaskLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                await SafeWait(triggerUpdateOperations, Timeout.InfiniteTimeSpan, token);
                triggerUpdateOperations.Reset();
                if (token.IsCancellationRequested) break;
                var newTasks = new List<Task>();

                bool restarted = false;
                if (restart)
                {
                    restarted = true;
                    newTasks.Add(extractor.FinishExtractorRestart());
                }
                else
                {
                    newTasks.Add(extractor.PushExtraNodes());
                }
                await Task.WhenAll(newTasks);
                if (restarted)
                {
                    restart = false;
                }
            }
        }

        private async Task HistoryRestartLoop(CancellationToken token)
        {
            var delay = config.History.RestartPeriod > 0 ? TimeSpan.FromSeconds(config.History.RestartPeriod) : Timeout.InfiniteTimeSpan;
            await SafeWait(triggerHistoryRestart, delay, token);
            while (!token.IsCancellationRequested)
            {
                triggerHistoryRestart.Reset();
                var waitTask = SafeWait(triggerHistoryRestart, delay, token);
                log.Information("Restarting history...");
                bool success = await extractor.TerminateHistory(30);
                if (!success) throw new ExtractorFailureException("Failed to terminate history");
                if (config.History.Enabled && config.History.Data)
                {
                    foreach (var state in extractor.State.NodeStates.Where(state => state.FrontfillEnabled))
                    {
                        state.RestartHistory();
                    }
                }
                if (config.Events.History && config.Events.Enabled)
                {
                    foreach (var state in extractor.State.EmitterStates.Where(state => state.FrontfillEnabled))
                    {
                        state.RestartHistory();
                    }
                }
                await extractor.RestartHistory();
                await waitTask;
            }
        }
        public void Dispose()
        {
            triggerUpdateOperations?.Dispose();
            triggerHistoryRestart?.Dispose();
            triggerGrowTaskList?.Dispose();
            triggerPush?.Dispose();
            triggerStoreState?.Dispose();
            triggerRebrowse?.Dispose();
        }

    }
}
