using Serilog;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using System.Threading;
using System.Threading.Tasks;

[assembly: CLSCompliant(false)]
namespace Server
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA1812:ServerOptions never instantiated",
        Justification = "Instantiated through reflection.")]
    internal class ServerOptions
    {
        public string EndpointUrl { get; set; }
        public int? Port { get; set; }
        public string MqttUrl { get; set; }
        public bool BaseHistory { get; set; }
        public bool CustomHistory { get; set; }
        public bool EventHistory { get; set; }
        public bool BasePeriodic { get; set; }
        public bool CustomPeriodic { get; set; }
        public bool EventsPeriodic { get; set; }
        public bool GrowthPeriodic { get; set; }
        public bool LargeHierarchy { get; set; }
        public bool VeryLargeHierarchy { get; set; }
        public bool Pubsub { get; set; }
        public bool Diagnostics { get; set; }
        public bool CoreProfile { get; set; }
        public int MaxBrowseResults { get; set; }
        public int MaxBrowseNodes { get; set; }
        public int MaxAttributes { get; set; }
        public int MaxSubscriptions { get; set; }
        public int MaxHistoryNodes { get; set; }
        public int RemainingBrowseCount { get; set; }
    }


    internal class Program
    {
        private static async Task<int> Main(string[] args)
        {
            var logConfig = new LoggerConfiguration();
            logConfig.MinimumLevel.Verbose();
            logConfig.WriteTo.Console();
            Log.Logger = logConfig.CreateLogger();

            return await GetCommandLineOptions().InvokeAsync(args);
        }

        private static ServerController BuildServer(ServerOptions opt)
        {
            var setups = new List<PredefinedSetup> { PredefinedSetup.Custom, PredefinedSetup.Base,
                PredefinedSetup.Events, PredefinedSetup.Wrong, PredefinedSetup.Auditing };
            if (opt.Pubsub) setups.Add(PredefinedSetup.PubSub);
            if (opt.LargeHierarchy) setups.Add(PredefinedSetup.Full);
            if (opt.VeryLargeHierarchy) setups.Add(PredefinedSetup.VeryLarge);

            int port = opt.Port ?? 62546;
            string endpointUrl = opt.EndpointUrl ?? "opc.tcp://localhost";
            string mqttUrl = opt.MqttUrl ?? "mqtt://localhost:4060";

            var controller = new ServerController(setups, port, mqttUrl, endpointUrl);

            return controller;
        }

        private static async Task Run(ServerOptions opt, ServerController server)
        {
            await server.Start();

            if (opt.Diagnostics) server.SetDiagnosticsEnabled(true);
            if (opt.BaseHistory || opt.CoreProfile) server.PopulateBaseHistory();
            if (opt.CustomHistory || opt.CoreProfile) server.PopulateCustomHistory();
            if (opt.EventHistory || opt.CoreProfile) server.PopulateEvents();

            server.Server.Issues.MaxBrowseResults = opt.MaxBrowseResults;
            server.Server.Issues.MaxBrowseNodes = opt.MaxBrowseNodes;
            server.Server.Issues.MaxAttributes = opt.MaxAttributes;
            server.Server.Issues.MaxSubscriptions = opt.MaxSubscriptions;
            server.Server.Issues.MaxHistoryNodes = opt.MaxHistoryNodes;
            server.Server.Issues.RemainingBrowseCount = opt.RemainingBrowseCount;

            int idx = 0;

            void ServerUpdate(object state)
            {
                if (opt.BasePeriodic || opt.CoreProfile) server.UpdateBaseNodes(idx);
                if (opt.CustomPeriodic || opt.CoreProfile) server.UpdateCustomNodes(idx);
                if (opt.EventsPeriodic || opt.CoreProfile) server.TriggerEvents(idx);

                if (opt.GrowthPeriodic)
                {
                    server.DirectGrowth(idx);
                    server.ReferenceGrowth(idx);
                }

                idx++;
            }

            using var timer = new Timer(ServerUpdate, null, 0, 1000);

            using var exitEvent = new ManualResetEvent(false);

            Console.CancelKeyPress += (sender, args) =>
            {
                args.Cancel = true;
                exitEvent.Set();
            };

            exitEvent.WaitOne();
        }

        private static Parser GetCommandLineOptions()
        {
            var root = new RootCommand
            {
                Description = "Cognite OPC-UA Test Server"
            };

            var option = new Option<string>("--endpoint-url",
                "Endpoint to run the server on, defaults to opc.tcp://localhost");
            option.AddAlias("-e");
            root.AddOption(option);

            var intOption = new Option<int>("--port", "Port to run the server on, defaults to 62546");
            intOption.AddAlias("-p");
            root.AddOption(intOption);

            option = new Option<string>("--mqtt-url", "Broker URL when publishing to mqtt. " +
                "Defaults to mqtt://localhost:4060 when pubsub is enabled");
            root.AddOption(option);

            var flag = new Option("--base-history", "Populate history for nodes in the 'Base' node hierarchy.");
            root.AddOption(flag);

            flag = new Option("--custom-history", "Populate history for nodes in the 'Custom' node hierarchy.");
            root.AddOption(flag);

            flag = new Option("--event-history", "Populate event history for all emitters.");
            root.AddOption(flag);

            flag = new Option("--base-periodic", "Periodically update nodes in the 'Base' node hierarchy.");
            root.AddOption(flag);

            flag = new Option("--custom-periodic", "Periodically update nodes in the 'Custom' node hierarchy.");
            root.AddOption(flag);

            flag = new Option("--events-periodic", "Periodically update nodes in the 'Events' node hierarchy.");
            root.AddOption(flag);

            flag = new Option("--growth-periodic", "Periodically add new nodes and references " +
                "in the 'Growing' node hierarchy.");
            root.AddOption(flag);

            flag = new Option("--large-hierarchy", "Create nodes from the wide and deep 'Full' node hierarchy.");
            root.AddOption(flag);

            flag = new Option("--very-large-hierarchy", "Create nodes from the very large 'VeryLarge' node hierarchy.");
            root.AddOption(flag);

            flag = new Option("--pubsub", "Create nodes for and load the 'PubSub' node hierarchy, and write to MQTT.");
            root.AddOption(flag);

            flag = new Option("--diagnostics", "Enable server diagnostics.");
            root.AddOption(flag);

            flag = new Option("--core-profile", "Enable periodic and history events and datapoints. " +
                "Equivalent to each -history flag, and base, custom, and events periodic flags");
            root.AddOption(flag);

            intOption = new Option<int>("--max-browse-results", "Server issue: Do not return continuation points, " +
                "and never return more references than this value.");
            root.AddOption(intOption);

            intOption = new Option<int>("--max-browse-nodes", "Server issue: More nodes than this value will" +
                " result in BadTooManyOperations.");
            root.AddOption(intOption);

            intOption = new Option<int>("--max-attributes", "Server issue: More attribute reads than this value " +
                " will result in BadTooManyOperations");
            root.AddOption(intOption);

            intOption = new Option<int>("--max-subscriptions", "Server issue: More monitored items created than this " +
                "value will result in BadTooManyOperations");
            root.AddOption(intOption);

            intOption = new Option<int>("--max-history-nodes", "Server issue: More nodes when reading history than" +
                " this value will result in BadTooManyOperations");
            root.AddOption(intOption);

            intOption = new Option<int>("--remaining-browse-count", "Server issue: This " +
                "number counts down for each browse operation, " +
                "and once it reaches zero it results in BadTooManyOperations");
            root.AddOption(intOption);

            root.Handler = CommandHandler.Create(async (ServerOptions opt) =>
            {
                using var controller = BuildServer(opt);
                await Run(opt, controller);
            });


            return new CommandLineBuilder(root)
                .UseHelp()
                .Build();
        }
    }
}
