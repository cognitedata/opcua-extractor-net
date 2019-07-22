using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public static class Logger
    {
        private static readonly ConcurrentDictionary<string, DateTime> LastLogged =
            new ConcurrentDictionary<string, DateTime>();

        private static readonly int MaxFormattedExceptionLength = 100_000;
        private static readonly object StartedMutex = new object();

        private static BlockingCollection<LogLine> LogLines = new BlockingCollection<LogLine>();
        private static Task backgroundTask;
        private static string logFolder;
        private static bool started;
		private static bool logData;
        private static bool logNodes;
        private static bool logConsole;

        private enum Severity
        {
            Verbose, Info, Warning, Error
        }

        public static void Startup(LoggerConfig options)
        {
            lock (StartedMutex)
            {
                if (started)
                {
                    return;
                }

                started = true;
            }

            if (options.LogFolder != null && options.LogFolder.Trim() != "")
            {
                logFolder = options.LogFolder;
            }

			logData = options.LogData;
            logNodes = options.LogNodes;
            logConsole = options.LogConsole;
            if (LogLines.IsAddingCompleted)
            {
                LogLines = new BlockingCollection<LogLine>();
            }
            backgroundTask = LogInBackground();
        }

        public static void Shutdown()
        {
            LogLines.CompleteAdding();
            backgroundTask.Wait();
            started = false;
        }

        private static async Task LogInBackground()
        {
            while (!LogLines.IsCompleted)
            {
                if (LogLines.TryTake(out var line))
                {
                    LogInternal(line);
                }
                else
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100));
                }
            }
        }

        public static void LogException(Exception e)
        {
            LogLines.Add(new LogLine { Time = DateTime.Now, Exception = e, Severity = Severity.Error });
        }

        public static void LogError(string msg)
        {
            LogLines.Add(new LogLine { Time = DateTime.Now, Message = msg, Severity = Severity.Error });
        }

        public static void LogWarning(string msg)
        {
            LogLines.Add(new LogLine { Time = DateTime.Now, Message = msg, Severity = Severity.Warning });
        }

        public static void LogInfo(string msg)
        {
            LogLines.Add(new LogLine { Time = DateTime.Now, Message = msg, Severity = Severity.Info });
        }

        public static void LogData(BufferedDataPoint dp)
		{
            if (logData)
			{
                LogLines.Add(new LogLine
                {
                    Time = DateTime.Now,
                    Message = $"Update {dp.Id}: {(dp.isString ? dp.stringValue : dp.doubleValue.ToString())} at {dp.timestamp}",
                    Severity = Severity.Verbose
                });
			}
		}

        public static void LogData(BufferedNode node)
		{
            if (logNodes)
            {
                LogLines.Add(new LogLine
                {
                    Time = DateTime.Now,
                    Message = $"Found node {node.Id}: {node.DisplayName}",
                    Severity = Severity.Verbose
                });
            }
		}

        public static void LogVerbose(string throttleId, string msg)
        {
            if (ShouldLog(throttleId))
            {
                LogLines.Add(new LogLine { Time = DateTime.Now, Message = msg, Severity = Severity.Verbose });
                DidLog(throttleId);
            }
        }

        private static bool ShouldLog(string throttleId)
        {
            if (!LastLogged.ContainsKey(throttleId))
            {
                return true;
            }

            return (DateTime.Now - LastLogged[throttleId]).TotalSeconds > 5;
        }

        private static void DidLog(string throttleId)
        {
            LastLogged[throttleId] = DateTime.Now;
        }

        private static string FormatException(Exception e)
        {
            StringBuilder builder = new StringBuilder();
            FormatException(e, builder);
            return builder.ToString();
        }

        private static void FormatException(Exception e, StringBuilder builder)
        {
            if (e is AggregateException aggregateException)
            {
                bool first = true;
                foreach (var innerException in aggregateException.InnerExceptions)
                {
                    if (builder.Length >= MaxFormattedExceptionLength)
                    {
                        builder.Append("\n...");
                        return;
                    }

                    if (first)
                    {
                        first = false;
                    }
                    else
                    {
                        builder.Append("\n\n");
                    }

                    FormatException(innerException, builder);
                }

                return;
            }

            FormatSingleException(e, builder);
            for (Exception inner = e.InnerException; inner != null; inner = e.InnerException)
            {
                builder.Append("\n");

                if (builder.Length >= MaxFormattedExceptionLength)
                {
                    builder.Append("...");
                    return;
                }

                FormatSingleException(inner, builder);
            }
        }

        private static void FormatSingleException(Exception e, StringBuilder builder)
        {
            builder.Append(e.GetType());
            builder.Append(": ");
            builder.Append(e.Message);
            builder.Append("\n");
            builder.Append(e.StackTrace);
        }

        private static void LogInternal(LogLine line)
        {
            string msg = line.Message ?? FormatException(line.Exception);

            string logMsg = $"{line.Time} {line.Severity.ToString().PadRight(7)} {msg}";
            if (logConsole)
            {
                ConsoleColor old = Console.ForegroundColor;
                Console.ForegroundColor = ToColor(line.Severity);
                Console.Out.WriteLine(logMsg);
                Console.ForegroundColor = old;
            }
            if (logFolder != null)
            {
                var hour = line.Time.Date.AddHours(line.Time.Hour);
                string hourString = hour.ToString("yyyy_MM_dd_HH");
                string path = logFolder + "\\" + hourString + ".log";
                try
                {
                    File.AppendAllLines(path, new[] { logMsg });
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine($"Error writing to file '{path}': {e}");
                }
            }
        }

        private static ConsoleColor ToColor(Severity severity)
        {
            switch (severity)
            {
                case Severity.Verbose: return ConsoleColor.Gray;
                case Severity.Info: return ConsoleColor.White;
                case Severity.Warning: return ConsoleColor.Yellow;
                case Severity.Error: return ConsoleColor.Red;
                default: return ConsoleColor.Magenta;
            }
        }

        private class LogLine
        {
            public DateTime Time { get; set; }

            public string Message { get; set; }

            public Severity Severity { get; set; }

            public Exception Exception { get; set; }
        }
    }
}
