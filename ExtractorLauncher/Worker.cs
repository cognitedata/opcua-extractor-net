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

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Service
{
    public class Worker : BackgroundService
    {
        private readonly ExtractorParams setup;
        private readonly Microsoft.Extensions.Logging.ILogger eventLog;
        public Worker(ILogger<Worker> eventLog, ExtractorParams setup)
        {
            this.setup = setup;
            this.eventLog = eventLog;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await ExtractorStarter.RunExtractor(eventLog, setup, stoppingToken);
        }
    }
}
