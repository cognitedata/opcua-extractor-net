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
using Prometheus;
using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public sealed class Looper2
    {
        private readonly UAExtractor extractor;
        private readonly FullConfig config;

        public PeriodicScheduler Scheduler { get; }

        private readonly IEnumerable<IPusher> pushers;
        private readonly ILogger log = Log.Logger.ForContext(typeof(Looper2));

        private TaskCompletionSource<bool>? pushWaiterSource;
        private bool restart;

        private List<IPusher> failingPushers = new List<IPusher>();
        private List<IPusher> passingPushers = new List<IPusher>();

        private static readonly Counter numPushes = Metrics.CreateCounter("opcua_num_pushes",
            "Increments by one after each push to destination systems");

        public Looper2(PeriodicScheduler scheduler, UAExtractor extractor, FullConfig config, IEnumerable<IPusher> pushers)
        {
            Scheduler = scheduler;
            this.extractor = extractor;
            this.config = config;
            this.pushers = pushers;
        }

        private static TimeSpan ToTimespan(int t, bool allowZero, string unit)
        {
            if (t < 0) return Timeout.InfiniteTimeSpan;
            if (t == 0 && !allowZero) return Timeout.InfiniteTimeSpan;
            switch (unit)
            {
                case "s":
                    return TimeSpan.FromSeconds(t);
                case "ms":
                    return TimeSpan.FromMilliseconds(t);
                case "m":
                    return TimeSpan.FromMinutes(t);
            }
            return TimeSpan.FromSeconds(t);
        }

        public Task Run(IEnumerable<Func<CancellationToken, Task>> synchTasks)
        {
            Scheduler.SchedulePeriodicTask(nameof(Pushers), ToTimespan(config.Extraction.DataPushDelay, true, "ms"), Pushers, true);
            Scheduler.SchedulePeriodicTask(nameof(ExtraTasks), Timeout.InfiniteTimeSpan, ExtraTasks, false);
            int idx = 0;
            foreach (var task in synchTasks)
            {
                Scheduler.ScheduleTask($"Synch{idx++}", task);
            }
            Scheduler.SchedulePeriodicTask(nameof(Rebrowse), ToTimespan(config.Extraction.AutoRebrowsePeriod, false, "m"), Rebrowse, false);
            if (extractor.StateStorage != null)
            {
                Scheduler.SchedulePeriodicTask(nameof(StoreState), ToTimespan(config.StateStorage.Interval, false, "s"), StoreState, 
                    config.StateStorage.Interval > 0);
            }
            Scheduler.SchedulePeriodicTask(nameof(HistoryRestart), ToTimespan(config.History.RestartPeriod, false, "s"), HistoryRestart, false);

            return Scheduler.WaitForAll();
        }

        /// <summary>
        /// Schedule a restart of the extractor.
        /// </summary>
        public void Restart()
        {
            restart = true;
            Scheduler.TriggerTask(nameof(ExtraTasks));
        }

        /// <summary>
        /// Wait for the next push of data to CDF
        /// </summary>
        /// <param name="timeout">Timeout in 1/10th of a second</param>
        public async Task WaitForNextPush(bool trigger = false, int timeout = 100)
        {
            pushWaiterSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (trigger)
            {
                Scheduler.TriggerTask(nameof(Pushers));
            }
            var t = new Stopwatch();
            t.Start();
            var waitTask = pushWaiterSource.Task;
            var task = await Task.WhenAny(waitTask, Task.Delay(timeout * 100));
            pushWaiterSource = null;
            if (task != waitTask) throw new TimeoutException("Waiting for push timed out");
            t.Stop();

            log.Debug("Waited {s} milliseconds for push", t.ElapsedMilliseconds);
        }


        private async Task Pushers(CancellationToken token)
        {
            if (token.IsCancellationRequested) return;
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

            var results = await Task.WhenAll(
                Task.Run(async () =>
                    await extractor.Streamer.PushDataPoints(passingPushers, failingPushers, token), token),
                Task.Run(async () => await extractor.Streamer.PushEvents(passingPushers, failingPushers, token), token));

            if (results.Any(res => res))
            {
                try
                {
                    Scheduler.TriggerTask(nameof(HistoryRestart));
                } catch { }
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

            numPushes.Inc();

            if (pushWaiterSource != null) pushWaiterSource.TrySetResult(true);
        }


        public async Task StoreState(CancellationToken token)
        {
            if (token.IsCancellationRequested) return;
            if (extractor.StateStorage == null) return;
            await Task.WhenAll(
                extractor.StateStorage.StoreExtractionState(extractor.State.NodeStates
                    .Where(state => state.FrontfillEnabled), config.StateStorage.VariableStore, token),
                extractor.StateStorage.StoreExtractionState(extractor.State.EmitterStates
                    .Where(state => state.FrontfillEnabled), config.StateStorage.EventStore, token)
            );
        }

        private async Task Rebrowse(CancellationToken token)
        {
            if (token.IsCancellationRequested) return;
            Console.WriteLine("Trigger rebrowse");
            await extractor.Rebrowse();
        }

        private async Task ExtraTasks(CancellationToken token)
        {
            if (token.IsCancellationRequested) return;
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

        private async Task HistoryRestart(CancellationToken token)
        {
            if (token.IsCancellationRequested) return;
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
        }
    }
}
