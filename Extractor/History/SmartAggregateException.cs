/* Cognite Extractor for OPC-UA
Copyright (C) 2023 Cognite AS

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
using System.Linq;
using System.Text;

namespace Cognite.OpcUa.History
{
    public class SmartAggregateException : Exception
    {
        private static string AggregateExceptionDesc(AggregateException aex)
        {
            var builder = new StringBuilder();
            var flattened = aex.Flatten();
            var byType = flattened.InnerExceptions
                .GroupBy(ex => ex.GetType())
                .ToDictionary(g => g.Key, v => v.Count());

            foreach (var (type, count) in byType)
            {
                builder.AppendFormat("{0} errors of type {1}", count, type);
                builder.AppendLine();
            }

            return builder.ToString();
        }

        public SmartAggregateException(AggregateException aex) : base(AggregateExceptionDesc(aex))
        {
        }
    }
}