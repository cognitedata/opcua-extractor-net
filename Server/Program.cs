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

using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils.CommandLine;
using Microsoft.Extensions.DependencyInjection;
using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Parsing;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

[assembly: CLSCompliant(false)]
namespace Server
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA1812:ServerOptions never instantiated",
        Justification = "Instantiated through reflection.")]
    internal sealed class ServerOptions
    {
        [CommandLineOption("Endpoint to run the server on, defaults to opc.tcp://localhost", true, "-e")]
        public string EndpointUrl { get; set; }
        [CommandLineOption("Port to run the server on, defaults to 62546", true, "-p")]
        public int? Port { get; set; }
        [CommandLineOption("Broker URL when publishing to mqtt. Defaults to mqtt://localhost:4060 when pubsub is enabled")]
        public string MqttUrl { get; set; }
        [CommandLineOption("Populate history for nodes in the 'Base' node hierarchy")]
        public bool BaseHistory { get; set; }
        [CommandLineOption("Populate history for nodes in the 'Custom' node hierarchy")]
        public bool CustomHistory { get; set; }
        [CommandLineOption("Populate event history for all emitters")]
        public bool EventHistory { get; set; }
        [CommandLineOption("Periodically update nodes in the 'Base' node hierarchy")]
        public bool BasePeriodic { get; set; }
        [CommandLineOption("Periodically update nodes in the 'Custom' node hierarchy")]
        public bool CustomPeriodic { get; set; }
        [CommandLineOption("Periodically update nodes in the 'Events' node hierarchy")]
        public bool EventsPeriodic { get; set; }
        [CommandLineOption("Periodically add new nodes and references in the 'Growing' node hierarchy")]
        public bool GrowthPeriodic { get; set; }
        [CommandLineOption("Server issue: Period in seconds between the server dropping all subscriptions from all connected sessions")]
        public int DropSubscriptions { get; set; }
        [CommandLineOption("Create nodes from the wide and deep 'Full' node hierarchy")]
        public bool LargeHierarchy { get; set; }
        [CommandLineOption("Create nodes from the very large 'VeryLarge' node hierarchy")]
        public bool VeryLargeHierarchy { get; set; }
        [CommandLineOption("Create nodes with complex object and variable types")]
        public bool Types { get; set; }
        [CommandLineOption("Create nodes for and load the 'PubSub' node hierarchy, and write to MQTT")]
        public bool Pubsub { get; set; }
        [CommandLineOption("Enable server diagnostics")]
        public bool Diagnostics { get; set; }
        [CommandLineOption("Enable periodic and history events and datapoints. " +
                "Equivalent to each -history flag, and base, custom, and events periodic flags")]
        public bool CoreProfile { get; set; }
        [CommandLineOption("Server issue: Do not return continuation points, " +
                "and never return more references than this value")]
        public int MaxBrowseResults { get; set; }
        [CommandLineOption("Server issue: More nodes than this value will" +
                " result in BadTooManyOperations")]
        public int MaxBrowseNodes { get; set; }
        [CommandLineOption("Server issue: More attribute reads than this value " +
                " will result in BadTooManyOperation")]
        public int MaxAttributes { get; set; }
        [CommandLineOption("Server issue: More monitored items created than this " +
                "value will result in BadTooManyOperations")]
        public int MaxSubscriptions { get; set; }
        [CommandLineOption("Server issue: More nodes when reading history than" +
                " this value will result in BadTooManyOperations")]
        public int MaxHistoryNodes { get; set; }
        [CommandLineOption("Server issue: This " +
                "number counts down for each browse operation, " +
                "and once it reaches zero it results in BadTooManyOperations")]
        public int RemainingBrowseCount { get; set; }
        [CommandLineOption("Level of logging to console. One of 'verbose', 'debug', 'information', 'warning', 'error' and 'fatal'", true, "-l")]
        public string LogLevel { get; set; }
        [CommandLineOption("Path to log files, this enables logging to file")]
        public string LogFile { get; set; }
        [CommandLineOption("Write OPC-UA SDK trace to log.")]
        public bool LogTrace { get; set; }

        [CommandLineOption("Manually set server service level, 0-255")]
        public byte ServiceLevel { get; set; } = 255;

        [CommandLineOption("Set server redundancy support. One of None, Cold, Warm, Hot, Transparent, HotAndMirrored")]
        public string RedundancySupport { get; set; }

        [CommandLineOption("Server issue: This is the denominator for a probability that an arbitrary operation will fail " +
            "I.e. 5 means that 1/5 browse ops will fail with BadInternalError")]
        public int RandomFail { get; set; }

        [CommandLineOption("List of NodeSet2 XML schemas to load the node hierarchy from, the base node hierarchy will not be loaded if this is specified. " +
            "May be specified more than once, the base OPC-UA nodeset should not be added.", true, "-s")]
        public IEnumerable<string> NodeSetFiles { get; set; }

        [CommandLineOption("Enable setting random status codes on values. Each chunk of 10 values will get the same code")]
        public bool RandomStatusCodes { get; set; }

        [CommandLineOption("Enable reading instructions from the CLI while running, to execute tasks dynamically")]
        public bool Interactive { get; set; }
    }


    internal sealed class Program
    {
        private static async Task<int> Main(string[] args)
        {
            return await GetCommandLineOptions().InvokeAsync(args);
        }

        private static ServerController BuildServer(IServiceProvider provider, ServerOptions opt)
        {
            List<PredefinedSetup> setups;
            if (opt.NodeSetFiles != null && opt.NodeSetFiles.Any())
            {
                setups = new List<PredefinedSetup>();
                if (opt.BaseHistory || opt.CoreProfile) setups.Add(PredefinedSetup.Base);
                if (opt.CustomHistory || opt.CoreProfile) setups.Add(PredefinedSetup.Custom);
                if (opt.EventHistory || opt.CoreProfile) setups.Add(PredefinedSetup.Events);
                if (opt.GrowthPeriodic) setups.Add(PredefinedSetup.Auditing);
            }
            else
            {
                setups = new List<PredefinedSetup> { PredefinedSetup.Custom, PredefinedSetup.Base,
                    PredefinedSetup.Events, PredefinedSetup.Wrong, PredefinedSetup.Auditing };
            }
            if (opt.Pubsub) setups.Add(PredefinedSetup.PubSub);
            if (opt.LargeHierarchy) setups.Add(PredefinedSetup.Full);
            if (opt.VeryLargeHierarchy) setups.Add(PredefinedSetup.VeryLarge);
            if (opt.Types) setups.Add(PredefinedSetup.Types);

            int port = opt.Port ?? 62546;
            string endpointUrl = opt.EndpointUrl ?? "opc.tcp://localhost";
            string mqttUrl = opt.MqttUrl ?? "mqtt://localhost:4060";

            var controller = new ServerController(setups, provider, port, mqttUrl, endpointUrl, opt.LogTrace, opt.NodeSetFiles);

            return controller;
        }

        private static async Task Run(ServerOptions opt, ServerController server)
        {
            await server.Start();

            if (opt.Diagnostics) server.SetDiagnosticsEnabled(true);
            if (opt.BaseHistory || opt.CoreProfile) server.PopulateBaseHistory(null, opt.RandomStatusCodes);
            if (opt.CustomHistory || opt.CoreProfile) server.PopulateCustomHistory(null, opt.RandomStatusCodes);
            if (opt.EventHistory || opt.CoreProfile) server.PopulateEvents();
            server.SetEventConfig(opt.GrowthPeriodic, opt.EventsPeriodic || opt.CoreProfile || opt.EventsPeriodic || opt.GrowthPeriodic, opt.GrowthPeriodic);

            server.Server.Issues.MaxBrowseResults = opt.MaxBrowseResults;
            server.Server.Issues.MaxBrowseNodes = opt.MaxBrowseNodes;
            server.Server.Issues.MaxAttributes = opt.MaxAttributes;
            server.Server.Issues.MaxMonitoredItems = opt.MaxSubscriptions;
            server.Server.Issues.MaxHistoryNodes = opt.MaxHistoryNodes;
            server.Server.Issues.RemainingBrowse = opt.RemainingBrowseCount;
            server.Server.Issues.RandomBrowseFailDenom = opt.RandomFail;
            server.Server.Issues.RandomBrowseNextFailDenom = opt.RandomFail;
            server.Server.Issues.RandomHistoryReadFailDenom = opt.RandomFail;
            server.Server.Issues.RandomReadFailDenom = opt.RandomFail;
            server.Server.Issues.RandomCreateMonitoredItemsFailDenom = opt.RandomFail;
            server.Server.Issues.RandomCreateSubscriptionsFailDenom = opt.RandomFail;

            if (opt.RedundancySupport != null)
            {
                server.SetServerRedundancyStatus(opt.ServiceLevel, Enum.Parse<Opc.Ua.RedundancySupport>(opt.RedundancySupport));
            }

            int idx = 0;


            var codeGen = ServerController.GetStatusGenerator();

            void ServerUpdate(object state)
            {
                StatusCode code;
                if (opt.RandomStatusCodes)
                {
                    code = codeGen(idx);
                }
                else
                {
                    code = StatusCodes.Good;
                }

                if (opt.BasePeriodic || opt.CoreProfile) server.UpdateBaseNodes(idx, code);
                if (opt.CustomPeriodic || opt.CoreProfile) server.UpdateCustomNodes(idx, code);
                if (opt.EventsPeriodic || opt.CoreProfile) server.TriggerEvents(idx);

                if (opt.GrowthPeriodic)
                {
                    server.DirectGrowth(idx);
                    server.ReferenceGrowth(idx);
                }

                if (opt.DropSubscriptions > 0 && idx % opt.DropSubscriptions == 0)
                {
                    server.Server.DropSubscriptions();
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

            if (opt.Interactive)
            {
                Console.WriteLine("Running in interactive mode. Instructions:");
                Console.WriteLine("    L: Set service level to 190");
                Console.WriteLine("    H: Set service level to 255");
                Console.WriteLine("    G: Set service level to 180");
                Console.WriteLine("    X: Kill all subscriptions on the server");
                var _ = Task.Run(() =>
                {
                    int sl = opt.ServiceLevel;
                    while (true)
                    {
                        var key = Console.ReadKey(true);
                        if (key.Key == ConsoleKey.L && sl != 190)
                        {
                            server.SetServerRedundancyStatus(190, Opc.Ua.RedundancySupport.Hot);
                            sl = 190;
                            Console.WriteLine("Set service level to 190");
                        }
                        if (key.Key == ConsoleKey.H && sl != 255)
                        {
                            server.SetServerRedundancyStatus(255, Opc.Ua.RedundancySupport.Hot);
                            sl = 255;
                            Console.WriteLine("Set service level to 255");
                        }
                        if (key.Key == ConsoleKey.G && sl != 180)
                        {
                            server.SetServerRedundancyStatus(180, Opc.Ua.RedundancySupport.Hot);
                            sl = 180;
                            Console.WriteLine("Set service level to 180");
                        }
                        if (key.Key == ConsoleKey.X)
                        {
                            server.Server.DropSubscriptions();
                        }
                    }
                });
            }


            exitEvent.WaitOne();
        }

        private static Parser GetCommandLineOptions()
        {
            var root = new RootCommand
            {
                Description = "Cognite OPC-UA Test Server"
            };

            var binder = new AttributeBinder<ServerOptions>();
            binder.AddOptionsToCommand(root);


            root.SetHandler<ServerOptions>(async opt =>
            {
                var loggerConfig = new LoggerConfig
                {
                    Console = new ConsoleConfig
                    {
                        Level = opt.LogLevel ?? "information"
                    }
                };

                if (opt.LogFile != null)
                {
                    loggerConfig.File = new FileConfig
                    {
                        Level = opt.LogLevel ?? "information",
                        Path = opt.LogFile
                    };
                }

                var services = new ServiceCollection();
                services.AddSingleton(loggerConfig);
                services.AddLogger();

                var provider = services.BuildServiceProvider();

                using var controller = BuildServer(provider, opt);
                await Run(opt, controller);
            }, binder);


            return new CommandLineBuilder(root)
                .UseHelp()
                .Build();
        }
    }
}
