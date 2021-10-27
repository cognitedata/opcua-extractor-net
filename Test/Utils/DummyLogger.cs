using Serilog;
using Serilog.Events;
using System.Collections.Generic;


namespace Test.Utils
{
    public class DummyLogger : ILogger
    {
        public List<LogEvent> Events { get; } = new List<LogEvent>();
        private object mutex = new object();
        public void Write(LogEvent logEvent)
        {
            lock (mutex)
            {
                Events.Add(logEvent);
            }
        }
    }
}
