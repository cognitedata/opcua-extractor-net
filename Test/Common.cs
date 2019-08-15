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
using System.Threading.Tasks;
using Cognite.OpcUa;

namespace Test
{
    public static class Common
    {
        public static FullConfig BuildConfig(string serverType, int index)
        {
            var fullConfig = Utils.GetConfig("config.test.yml");
            if (fullConfig == null) throw new Exception("Failed to load config file");
            fullConfig.CogniteConfig.BufferFile = $"buffer{index}.bin";
            if (serverType == "basic")
            {
                fullConfig.UAConfig.EndpointURL = "opc.tcp://localhost:4840";
            }
            else if (serverType == "full")
            {
                fullConfig.UAConfig.EndpointURL = "opc.tcp://localhost:4841";
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
    }
}
