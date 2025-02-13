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
using Microsoft.Extensions.Logging;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public sealed class Looper
    {
        private readonly UAExtractor extractor;
        private readonly FullConfig config;

        public PeriodicScheduler Scheduler { get; }

        private readonly IPusher pusher;
        private readonly ILogger<Looper> log;

        private TaskCompletionSource<bool>? pushWaiterSource;
        private bool restart;

        private static readonly Counter numPushes = Metrics.CreateCounter("opcua_num_pushes",
            "Increments by one after each push to destination systems");

        public Looper(
            ILogger<Looper> log,
            PeriodicScheduler scheduler,
            UAExtractor extractor,
            FullConfig config,
            IPusher pusher)
        {
            this.log = log;
            Scheduler = scheduler;
            this.extractor = extractor;
            this.config = config;
            this.pusher = pusher;
        }

        public void Run()
        {
            Scheduler.SchedulePeriodicTask(nameof(Pusher), config.Extraction.DataPushDelayValue.Value, Pusher, true);
            Scheduler.SchedulePeriodicTask(nameof(ExtraTasks), Timeout.InfiniteTimeSpan, ExtraTasks, false);

            Scheduler.SchedulePeriodicTask(nameof(Rebrowse), config.Extraction.AutoRebrowsePeriodValue, Rebrowse, false);
            if (extractor.StateStorage != null && !config.DryRun)
            {
                var interval = config.StateStorage.IntervalValue.Value;
                Scheduler.SchedulePeriodicTask(nameof(StoreState), interval, StoreState, interval != Timeout.InfiniteTimeSpan);
            }
            Scheduler.SchedulePeriodicTask(nameof(HistoryRestart), config.History.RestartPeriodValue, HistoryRestart, false);
        }

        /// <summary>
        /// Trigger a rebrowse.
        /// </summary>
        public void QueueRebrowse()
        {
            Scheduler.TryTriggerTask(nameof(Rebrowse));
        }

        /// <summary>
        /// Schedule a restart of the extractor.
        /// </summary>
        public void Restart()
        {
            restart = true;
            Scheduler.TryTriggerTask(nameof(ExtraTasks));
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
                Scheduler.TryTriggerTask(nameof(Pusher));
            }
            var t = new Stopwatch();
            t.Start();
            var waitTask = pushWaiterSource.Task;
            var task = await Task.WhenAny(waitTask, Task.Delay(timeout * 100));
            pushWaiterSource = null;
            if (task != waitTask) throw new TimeoutException("Waiting for push timed out");
            t.Stop();

            log.LogDebug("Waited {MS} milliseconds for push", t.ElapsedMilliseconds);
        }


        private async Task EnsurePusherIsInitialized(CancellationToken token)
        {
            if (pusher.Initialized) return;

            var result = await pusher.TestConnection(config, token);

            if (result != true)
            {
                return;
            }

            log.LogInformation("Pusher connection test succeeded, attempting to initialize");

            pusher.NoInit = false;
            var pending = pusher.PendingNodes;
            pusher.PendingNodes = null;
            await extractor.PushNodes(pending, pusher, true);

            if (pusher.Initialized)
            {
                pusher.DataFailing = extractor.Streamer.AllowData;
                pusher.EventsFailing = extractor.Streamer.AllowEvents;
            }
        }


        private async Task Pusher(CancellationToken token)
        {
            if (token.IsCancellationRequested) return;

            await EnsurePusherIsInitialized(token);

            await Task.WhenAll(
                Task.Run(async () => await extractor.Streamer.PushDataPoints(pusher, token), token),
                Task.Run(async () => await extractor.Streamer.PushEvents(pusher, token), token)
            );

            numPushes.Inc();
            pushWaiterSource?.TrySetResult(true);
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

        private void HistoryRestart(CancellationToken token)
        {
            if (token.IsCancellationRequested) return;
            log.LogInformation("Requesting restart of history...");

            extractor.TriggerHistoryRestart();
        }
    }
}
