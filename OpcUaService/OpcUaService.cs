using System;
using System.Diagnostics;
using System.IO;
using System.ServiceProcess;
using System.Threading;
using System.Timers;

namespace OpcUaService
{
    public partial class OpcUaWindowsService : ServiceBase
    {
        // Private vars
        private const string _eventLogSource = "CogniteOpcUa";
        private string _eventLogPrefix = "DefaultService";
        private const string _configFileName = @"config\config.yml";
        private static Thread _extractorThread;
        private static bool _isFirstStart = true;
        private static int _numberOfFailures;
        private static int _maxNumberOfFailures = 3;
        private static System.Timers.Timer _timer;
        private const double _statusCheckInterval = 10000;
        private string _path;
        private string _configFile;
        private string[] _args;

        /// <summary>
        /// Initializing the service
        /// </summary>
        public OpcUaWindowsService(string[] args)
        {
            InitializeComponent();
            _args = args;
            _numberOfFailures = 0;
        }

        /// <summary>
        /// Called on windows service Start
        /// </summary>
        /// <param name="args"></param>
        protected override void OnStart(string[] args)
        {
            // Add eventlog source for the extractor service
            if (!EventLog.SourceExists(_eventLogSource))
                EventLog.CreateEventSource(_eventLogSource, "Application");

            // Check for config file, either from path passed in as argument or location of the service exe file
            if (_args.Length > 0)
            {
                _path = _args[0];
                if (!Directory.Exists(_path))
                {
                    LogMessage($"The working directory does not exist: {_path}", EventLogEntryType.Error);
                    Stop();
                    Environment.Exit(1);
                }

                if (_args[1] != null && !string.IsNullOrWhiteSpace(_args[1]))
                {
                    _eventLogPrefix = _args[1];
                }
            }
            else
            {
                _path = Directory.GetParent(typeof(OpcUaWindowsService).Assembly.Location).ToString();
            }

            _configFile = $@"{_path}\{_configFileName}";

            // Validates the config file, and that it can be loaded and used. 
            string configFileTest = ExtractorServiceStarter.ValidateConfigurationFile(_configFile);

            if (configFileTest != "OK")
            {
                LogMessage(configFileTest, EventLogEntryType.Error);
                Stop();
                Environment.Exit(1);
            }


            LogMessage("Initial startup of OpcUa Extractor service.", EventLogEntryType.Information);

            // The Extractor needs this to be set, to find config and log folder if not full paths is specified in config file
            Directory.SetCurrentDirectory(_path);

            // Setup a thread for the extractor process
            _extractorThread = new Thread(new ThreadStart(() => ExtractorServiceStarter.Start(_configFile)));
            _extractorThread.IsBackground = true;

            // Setup timer job
            _timer = new System.Timers.Timer(_statusCheckInterval);
            _timer.Elapsed += CheckExtractorStatus;
            _timer.AutoReset = true;
            _timer.Start();
        }

        /// <summary>
        /// Called on windows service Stop
        /// </summary>
        ///
        protected override void OnStop()
        {
            if (_timer != null)
            {
                _timer.Stop();
                _timer.Dispose();
            }

            if (ExtractorServiceStarter.RunningStatus())
            {
                ExtractorServiceStarter.Stop();
            }

            if (_extractorThread != null)
            {
                while (ExtractorServiceStarter.RunningStatus() || _extractorThread.IsAlive)
                {
                    Thread.Sleep(500);
                }
            }

            LogMessage("Stopped the OpcUa Extractor process.", EventLogEntryType.Information);
        }

        /// <summary>
        /// Add init stuff here
        /// </summary>
        private void InitializeComponent()
        {
            ServiceName = "OpcUaService";
        }


        /// <summary>
        /// Does initial start and checks status of extractor task, and tries restart it on failures.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void CheckExtractorStatus(object sender, ElapsedEventArgs e)
        {
            _timer.Enabled = false;

            try
            {
                if (_isFirstStart)
                {
                    _isFirstStart = false;
                    _extractorThread.Start();
                    // Allow it some time to start, and set the running status bool flag.
                    Thread.Sleep(3000);
                }

                bool isExtractorRunning = ExtractorServiceStarter.RunningStatus();

                if (!isExtractorRunning || !_extractorThread.IsAlive)
                {
                    if (_numberOfFailures < _maxNumberOfFailures)
                    {
                        LogMessage($"Restarting OpcUa-Extractor, this has happened: {_numberOfFailures} times", EventLogEntryType.Error);
                        SleepOnError();
                        _extractorThread = new Thread(new ThreadStart(() => ExtractorServiceStarter.Start(_configFile)));
                        _extractorThread.Start();
                    }
                    else
                    {
                        LogMessage($"OpcUa-Extractor process has failed {_numberOfFailures} times. Exiting the service.", EventLogEntryType.Error);
                        OnStop();
                        Environment.Exit(1);
                    }

                    _numberOfFailures++;
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Exception Message: {ex.Message} -- Inner Exception: {ex.InnerException}", EventLogEntryType.Error);
                LogMessage("Calling Service STOP and exiting.", EventLogEntryType.Information);
                OnStop();
                Environment.Exit(1);
            }

            _timer.Enabled = true;
        }

        /// <summary>
        /// Wait X amount of time based on number of failures
        /// </summary>
        private static void SleepOnError()
        {
            switch (_numberOfFailures)
            {
                case 0:
                    Thread.Sleep(10000);
                    break;
                case 1:
                    Thread.Sleep(30000);
                    break;
                case 2:
                    Thread.Sleep(60000);
                    break;
                default:
                    Thread.Sleep(120000);
                    break;
            }
        }

        /// <summary>
        /// Logs a message to the eventlog
        /// </summary>
        /// <param name="message"></param>
        /// <param name="level"></param>
        private void LogMessage(string message, EventLogEntryType level)
        {
            EventLog eventLog = new EventLog();
            eventLog.Source = _eventLogSource;

            try
            {
                eventLog.WriteEntry(_eventLogPrefix + ": " + message, level);
                //File.AppendAllText(@"c:\opcua\logs\debug.txt", message + "\r\n");
            }
            catch (Exception ex)
            {
                throw new Exception("Failed when writing to eventlog", ex.InnerException);
            }
            finally
            {
                eventLog.Dispose();
            }
        }


    }
}


