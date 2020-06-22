using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Serilog;

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
        private bool quit;

        private IEnumerable<Task> tasks;
        private readonly IEnumerable<IPusher> pushers;
        private readonly ILogger log = Log.Logger.ForContext(typeof(Looper));
        private readonly AutoResetEvent triggerUpdateOperations = new AutoResetEvent(false);
        private readonly ManualResetEvent triggerHistoryRestart = new ManualResetEvent(false);
        private readonly ManualResetEvent triggerGrowTaskList = new ManualResetEvent(false);

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
        public async Task WaitForNextPush(int timeout = 100)
        {
            nextPushFlag = false;
            int time = 0;
            while (!nextPushFlag && time++ < timeout) await Task.Delay(100);
            if (time >= timeout && !nextPushFlag)
            {
                throw new TimeoutException("Waiting for push timed out");
            }
            log.Debug("Waited {s} milliseconds for push", time * 100);
        }
        /// <summary>
        /// Main task loop, terminates on any task failure or if all tasks are finished.
        /// </summary>
        /// <param name="synchTasks">Initial history tasks</param>
        public async Task InitTaskLoop(IEnumerable<Task> synchTasks, CancellationToken token)
        {
            tasks = new List<Task>
            {
                Task.Run(async () => await PushersLoop(token), token),
                Task.Run(async () => await ExtraTaskLoop(token), token)
            }.Concat(synchTasks);

            if (config.Extraction.AutoRebrowsePeriod > 0)
            {
                tasks = tasks.Append(Task.Run(() => RebrowseLoop(token)));
            }

            if (extractor.StateStorage != null && config.StateStorage.Interval > 0)
            {
                tasks = tasks.Append(Task.Run(() => StoreStateLoop(token)));
            }

            tasks = tasks.Append(Task.Run(() => WaitHandle.WaitAny(
                new[] { triggerHistoryRestart, token.WaitHandle })));
            tasks = tasks.Append(Task.Run(() => WaitHandle.WaitAny(
                new[] { triggerGrowTaskList, token.WaitHandle })));

            tasks = tasks.ToList();

            triggerUpdateOperations.Reset();
            var failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);
            if (failedTask != null)
            {
                ExtractorUtils.LogException(failedTask.Exception, "Unexpected error in main task list", "Handled error in main task list");
            }

            while (tasks.Any() && failedTask == null)
            {

                try
                {
                    var terminated = await Task.WhenAny(tasks);
                    if (terminated.IsFaulted)
                    {
                        ExtractorUtils.LogException(terminated.Exception,
                            "Unexpected error in main task list",
                            "Handled error in main task list");
                    }
                }
                catch (Exception ex)
                {
                    ExtractorUtils.LogException(ex, "Unexpected error in main task list", "Handled error in main task list");
                }
                failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);

                if (failedTask != null) break;
                tasks = tasks
                    .Where(task => !task.IsCompleted && !task.IsFaulted && !task.IsCanceled)
                    .ToList();

                if (triggerHistoryRestart.WaitOne(0))
                {
                    triggerHistoryRestart.Reset();
                    tasks = tasks.Append(extractor.RestartHistory(token)).Append(Task.Run(() => WaitHandle.WaitAny(
                        new[] { triggerHistoryRestart, token.WaitHandle }))).ToList();
                }

                if (triggerGrowTaskList.WaitOne(0))
                {
                    triggerGrowTaskList.Reset();
                    tasks = tasks.Append(Task.Run(() => WaitHandle.WaitAny(
                        new[] { triggerGrowTaskList, token.WaitHandle }))).ToList();
                }
            }

            if (token.IsCancellationRequested) throw new TaskCanceledException();
            if (failedTask != null)
            {
                if (failedTask.Exception != null)
                {
                    ExceptionDispatchInfo.Capture(failedTask.Exception).Throw();
                }

                throw new ExtractorFailureException("Task failed without exception");
            }
            throw new ExtractorFailureException("Processes quit without failing");
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

                    if (recovered.Any(pair => !pair.pusher.Initialized))
                    {
                        log.Information("Pushers with indices {idx} recovered",
                            recovered.Select(val => val.pusher.Index.ToString(CultureInfo.InvariantCulture))
                                .Aggregate((src, val) => src + ", " + val));

                        var toInit = recovered.Select(pair => pair.pusher).Where(pusher => !pusher.Initialized);
                        var (nodes, timeseries) = ExtractorUtils.SortNodes(extractor.State.ActiveNodes);
                        await Task.WhenAll(toInit.Select(pusher => extractor.PushNodes(nodes, timeseries, pusher, true, true, token)));
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

                var waitTask = Task.Delay(config.Extraction.DataPushDelay, token);
                var results = await Task.WhenAll(
                    Task.Run(async () =>
                        await extractor.Streamer.PushDataPoints(passingPushers, failingPushers, token), token),
                    Task.Run(async () => await extractor.Streamer.PushEvents(passingPushers, failingPushers, token), token));

                if (results.Any(res => res))
                {
                    triggerHistoryRestart.Set();
                }

                foreach (var pusher in passingPushers.Where(pusher =>
                    pusher.DataFailing && extractor.Streamer.AllowData || pusher.EventsFailing && extractor.Streamer.AllowEvents
                                                                       || !pusher.Initialized).ToList())
                {
                    pusher.DataFailing = extractor.Streamer.AllowData;
                    pusher.EventsFailing = extractor.Streamer.AllowEvents;
                    failingPushers.Add(pusher);
                    passingPushers.Remove(pusher);
                }

                await waitTask;
                nextPushFlag = true;
            }
        }

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
                await Task.WhenAll(
                    Task.Delay(delay, token),
                    StoreState(token));
            }
        }
        /// <summary>
        /// Loop for periodically browsing the UA hierarchy and adding subscriptions to any new nodes.
        /// </summary>
        private async Task RebrowseLoop(CancellationToken token)
        {
            var delay = TimeSpan.FromMinutes(config.Extraction.AutoRebrowsePeriod);
            while (!token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(delay, token);
                }
                catch (TaskCanceledException)
                {
                    return;
                }

                await extractor.Rebrowse(token);
            }
        }
        /// <summary>
        /// Waits for triggerUpdateOperations to fire, then executes all the tasks in the queue.
        /// </summary>
        private async Task ExtraTaskLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                WaitHandle.WaitAny(new[] { triggerUpdateOperations, token.WaitHandle });
                if (token.IsCancellationRequested) break;
                var newTasks = new List<Task>();
                if (quit)
                {
                    log.Warning("Manually quitting extractor due to error in subsystem");
                    quit = false;
                    throw new ExtractorFailureException("Manual exit due to error in subsystem");
                }

                bool restarted = false;
                if (restart)
                {
                    restarted = true;
                    newTasks.Add(extractor.FinishExtractorRestart(token));
                }
                else
                {
                    newTasks.Add(extractor.PushExtraNodes(token));
                }
                await Task.WhenAll(newTasks);
                if (restarted)
                {
                    restart = false;
                }
            }
        }

        public void Dispose()
        {
            triggerUpdateOperations?.Dispose();
            triggerHistoryRestart?.Dispose();
            triggerGrowTaskList?.Dispose();
        }
        /// <summary>
        /// Schedule quitting the extractor.
        /// </summary>
        public void Quit()
        {
            quit = true;
            triggerUpdateOperations.Set();
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
            tasks = tasks.Concat(newTasks);
            triggerGrowTaskList.Set();
        }
    }
}
