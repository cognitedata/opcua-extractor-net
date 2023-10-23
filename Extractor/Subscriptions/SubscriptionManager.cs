﻿using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Subscriptions
{
    public abstract class PendingSubscriptionTask
    {
        public abstract string TaskName { get; }

        public abstract Task<bool> ShouldRun(ILogger logger, SessionManager sessionManager, CancellationToken token);

        public abstract Task Run(ILogger logger, SessionManager sessionManager, FullConfig config, SubscriptionManager subscriptionManager, CancellationToken token);

        public ManualResetEvent TaskFinishedEvent { get; } = new ManualResetEvent(false);
    }

    public class SubscriptionManager
    {
        private readonly SessionManager sessionManager;
        private readonly FullConfig config;

        private readonly ILogger logger;

        private readonly object taskQueueLock = new();
        private readonly Queue<PendingSubscriptionTask> taskQueue = new();
        private readonly AutoResetEvent taskQueueEvent = new AutoResetEvent(false);

        public SubscriptionStateCache Cache { get; } = new();

        public SubscriptionManager(SessionManager sessionManager, FullConfig config, ILogger logger)
        {
            this.sessionManager = sessionManager;
            this.config = config;
            this.logger = logger;
        }

        public void OnSubscriptionPublishStatusChange(object sender, EventArgs e)
        {
            if (sender is not Subscription sub || !sub.PublishingStopped) return;

            EnqueueTaskEnsureUnique(new RecreateSubscriptionTask(sub));
        }

        public void EnqueueTaskEnsureUnique(PendingSubscriptionTask task)
        {
            lock (taskQueueLock)
            {
                if (taskQueue.Any(q => q.TaskName == task.TaskName)) return;
                taskQueue.Enqueue(task);
                taskQueueEvent.Set();
            }
        }

        public void EnqueueTask(PendingSubscriptionTask task)
        {
            lock (taskQueueLock)
            {
                taskQueue.Enqueue(task);
                taskQueueEvent.Set();
            }
        }

        public async Task EnqueueTaskAndWait(PendingSubscriptionTask task, CancellationToken token)
        {
            EnqueueTask(task);

            await CommonUtils.WaitAsync(task.TaskFinishedEvent, Timeout.InfiniteTimeSpan, token);
        }

        public async Task WaitForAllCurrentlyPendingTasks(CancellationToken token)
        {
            Task[] tasks;
            lock (taskQueueLock)
            {
                tasks = taskQueue.Select(t => CommonUtils.WaitAsync(t.TaskFinishedEvent, Timeout.InfiniteTimeSpan, token)).ToArray();
            }

            if (!tasks.Any()) return;
            await Task.WhenAll(tasks);
        }

        private async Task RunTask(PendingSubscriptionTask task, CancellationToken token)
        {
            try
            {
                if (!await task.ShouldRun(logger, sessionManager, token)) return;

                await task.Run(logger, sessionManager, config, this, token);
            }
            finally
            {
                task.TaskFinishedEvent.Set();
            }

        }

        public async Task RunTaskLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                PendingSubscriptionTask? task = null;
                lock (taskQueueLock)
                {
                    taskQueue.TryDequeue(out task);
                }

                if (task == null)
                {
                    await CommonUtils.WaitAsync(taskQueueEvent, Timeout.InfiniteTimeSpan, token);
                }
                else
                {
                    try
                    {
                        await RunTask(task, token);
                    }
                    catch (Exception ex)
                    {
                        ExtractorUtils.LogException(logger, ex, $"Failed subscription task: {task.TaskName}");
                    }
                }
            }
        }
    }
}
