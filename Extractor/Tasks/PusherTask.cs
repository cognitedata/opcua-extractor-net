using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils.Unstable.Tasks;
using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Prometheus;

namespace Cognite.OpcUa.Tasks
{
    public class PusherTask : BaseSchedulableTask
    {
        public override bool ErrorIsFatal => true;

        public override string Name => "Push data to CDF";

        public override TaskMetadata Metadata => new TaskMetadata(CogniteSdk.Alpha.TaskType.continuous);

        private readonly UAExtractor extractor;
        private readonly IPusher pusher;
        private readonly ILogger<PusherTask> log;
        private readonly FullConfig config;
        private PeriodicScheduler? scheduler;
        private TaskCompletionSource<bool>? pushWaiterSource;

        private static readonly Counter numPushes = Metrics.CreateCounter("opcua_num_pushes",
            "Increments by one after each push to destination systems");

        public PusherTask(UAExtractor extractor, FullConfig config, IPusher pusher, ILogger<PusherTask> log)
        {
            this.extractor = extractor;
            this.pusher = pusher;
            this.log = log;
            this.config = config;
        }

        public override bool CanRunNow()
        {
            // We can always just run this in the background.
            return true;
        }

        public void TriggerPushNow()
        {
            scheduler?.TryTriggerTask(nameof(Pusher));
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


        /// <summary>
        /// Wait for the next push of data to CDF
        /// </summary>
        /// <param name="timeout">Timeout in 1/10th of a second</param>
        public async Task WaitForNextPush(bool trigger = false, int timeout = 100)
        {
            pushWaiterSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (trigger)
            {
                scheduler?.TryTriggerTask(nameof(Pusher));
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

        public override async Task<TaskUpdatePayload?> Run(BaseErrorReporter task, CancellationToken token)
        {
            scheduler = new PeriodicScheduler(token);
            scheduler.SchedulePeriodicTask(nameof(Pusher), config.Extraction.DataPushDelayValue.Value, Pusher, true);

            await scheduler.WaitForAll();

            return null;
        }
    }
}