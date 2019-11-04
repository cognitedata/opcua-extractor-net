/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Microsoft.Extensions.DependencyInjection;
using Prometheus.Client;
using Xunit.Abstractions;

namespace Test
{
    public class MakeConsoleWork : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly TextWriter _originalOut;
        private readonly TextWriter _textWriter;

        public MakeConsoleWork(ITestOutputHelper output)
        {
            _output = output;
            _originalOut = Console.Out;
            _textWriter = new StringWriter();
            Console.SetOut(_textWriter);
        }

        public void Dispose()
        {
            _output.WriteLine(_textWriter.ToString());
            Console.SetOut(_originalOut);
        }
    }
    public static class Common
    {
        public static FullConfig BuildConfig(string serverType, int index, string configname = "config.test.yml")
        {
            var fullConfig = Utils.GetConfig(configname);
            if (fullConfig == null) throw new Exception("Failed to load config file");
            if (fullConfig.Pushers.First() is CogniteClientConfig cogniteConfig)
            {
                cogniteConfig.BufferFile = $"buffer{index}.bin";
            }
            if (serverType == "basic")
            {
                fullConfig.Source.EndpointURL = "opc.tcp://localhost:4840";
            }
            else if (serverType == "full")
            {
                fullConfig.Source.EndpointURL = "opc.tcp://localhost:4841";
            }
            else if (serverType == "array")
            {
                fullConfig.Source.EndpointURL = "opc.tcp://localhost:4842";
            }
            else if (serverType == "events")
            {
                fullConfig.Source.EndpointURL = "opc.tcp://localhost:4843";
            }
            else if (serverType == "audit")
            {
                fullConfig.Source.EndpointURL = "opc.tcp://localhost:4844";
            }
            return fullConfig;
        }
        public static bool TestRunResult(Exception e)
        {
            if (!(e is TaskCanceledException || e is AggregateException && e.InnerException is TaskCanceledException))
            {
                return false;
            }
            return true;
        }
        public static IServiceProvider GetDummyProvider(CDFMockHandler handler)
        {
            var services = new ServiceCollection();
            services.AddHttpClient<DataCDFClient>()
                .ConfigurePrimaryHttpMessageHandler(() => handler.GetHandler());
            services.AddHttpClient<ContextCDFClient>()
                .ConfigurePrimaryHttpMessageHandler(() => handler.GetHandler());
            return services.BuildServiceProvider();
        }

        public static double GetMetricValue(string name)
        {
            Metrics.DefaultCollectorRegistry.TryGet(name, out var collector);
            return collector switch
            {
                Gauge gauge => gauge.Value,
                Counter counter => counter.Value,
                _ => 0
            };
        }
        public static bool TestMetricValue(string name, double value)
        {
            Metrics.DefaultCollectorRegistry.TryGet(name, out var collector);
            return collector switch
            {
                Gauge gauge => (Math.Abs(gauge.Value - value) < 0.01),
                Counter counter => (Math.Abs(counter.Value - value) < 0.01),
                _ => false
            };
        }

        public static bool VerifySuccessMetrics()
        {
            return TestMetricValue("opcua_attribute_request_failures", 0)
                && TestMetricValue("opcua_history_read_failures", 0)
                && TestMetricValue("opcua_browse_failures", 0);
        }

        private static void ResetMetricValue(string name)
        {
            Metrics.DefaultCollectorRegistry.TryGet(name, out var collector);
            switch (collector)
            {
                case Gauge gauge:
                    gauge.Set(0);
                    break;
                case Counter counter:
                    counter.Reset();
                    break;
            }
        }
        public static void ResetTestMetrics()
        {
            var metrics = new List<string>
            {
                "opcua_attribute_request_failures", "opcua_history_read_failures", "opcua_browse_failures",
                "opcua_browse_operations", "opcua_history_reads", "opcua_tracked_timeseries",
                "opcua_tracked_assets", "opcua_node_ensure_failures", "opcua_datapoint_pushes",
                "opcua_datapoint_push_failures"
            };
            foreach (var metric in metrics)
            {
                ResetMetricValue(metric);
            }
        }
    }
}
