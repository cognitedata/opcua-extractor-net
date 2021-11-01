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

using Cognite.Extractor.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Prometheus;
using Serilog;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Service
{
    public class Worker : BackgroundService
    {
        private readonly ExtractorParams setup;
        private Microsoft.Extensions.Logging.ILogger eventLog;
        private ServiceCollection services;
        public Worker(ILogger<Worker> eventLog, ServiceCollection services, ExtractorParams setup)
        {
            this.setup = setup;
            this.eventLog = eventLog;
            this.services = services;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await ExtractorStarter.RunExtractor(eventLog, setup, services, stoppingToken);
        }
    }
}
