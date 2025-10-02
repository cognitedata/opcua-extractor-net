using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk;
using CogniteSdk.Alpha;

namespace Test.Utils
{
    public enum TaskEventType
    {
        Start,
        End
    }

    public class TaskEvent
    {
        public TaskEventType EventType { get; set; }
        public string TaskName { get; set; }
        public DateTime Timestamp { get; set; }
    }

    public class DummyIntegrationSink : IIntegrationSink
    {
        public List<ExtractorError> ReportedErrors { get; } = new List<ExtractorError>();
        public List<TaskEvent> TaskEvents { get; } = new List<TaskEvent>();

        public Task Flush(CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public void ReportError(ExtractorError error)
        {
            ReportedErrors.Add(error);
        }

        public void ReportTaskEnd(string taskName, TaskUpdatePayload update = null, DateTime? timestamp = null)
        {
            TaskEvents.Add(new TaskEvent
            {
                EventType = TaskEventType.End,
                TaskName = taskName,
                Timestamp = timestamp ?? DateTime.UtcNow
            });
        }

        public void ReportTaskStart(string taskName, TaskUpdatePayload update = null, DateTime? timestamp = null)
        {
            TaskEvents.Add(new TaskEvent
            {
                EventType = TaskEventType.Start,
                TaskName = taskName,
                Timestamp = timestamp ?? DateTime.UtcNow
            });
        }

        public Task RunPeriodicCheckIn(CancellationToken token, StartupRequest startupPayload, TimeSpan? interval = null)
        {
            // Needs to return a task that runs until canceled.
            return CommonUtils.WaitAsync(token.WaitHandle, Timeout.InfiniteTimeSpan, token);
        }
    }
}