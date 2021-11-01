using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;


namespace Test.Utils
{
    public class LogEvent
    {
        public LogLevel LogLevel { get; set; }
        public EventId EventId { get; set; }
        public Exception Exception { get; set; }
    }

    public class DummyLogger : ILogger
    {
        public List<LogEvent> Events { get; } = new List<LogEvent>();
        private readonly object mutex = new object();


        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception exception, Func<TState, Exception, string> formatter)
        {

            lock (mutex)
            {
                Events.Add(new LogEvent
                {
                    LogLevel = logLevel,
                    EventId = eventId,
                    Exception = exception
                });
            }
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            throw new NotImplementedException();
        }
    }
}
