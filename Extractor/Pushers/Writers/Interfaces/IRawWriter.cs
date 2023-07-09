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

using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.Writers.Dtos;
using Cognite.OpcUa.Types;
using CogniteSdk;

namespace Cognite.OpcUa.Pushers.Writers.Interfaces
{
    public interface IRawWriter
    {
        static JsonSerializerOptions options =>
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

        /// <summary>
        /// Get all rows from CDF
        /// </summary>
        /// <param name="dbName">Name of metadata database in CDF</param>
        /// <param name="tableName"> Name of metadata table in CDF</param>
        /// <param name="columns">Columns</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A dictionary of JsonElement</returns>
        Task<IEnumerable<RawRow<Dictionary<string, JsonElement>>>> GetRows(
            string dbName,
            string tableName,
            IEnumerable<string>? columns,
            CancellationToken token
        );

        /// <summary>
        /// Synchronizes all BaseUANode to CDF raw
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="database">Name of metadata database in CDF</param>
        /// <param name="table">Name of metadata table in CDF</param>
        /// <param name="rows">Dictionary map of BaseUANode of their keys</param>
        /// <param name="converter">Converter</param>
        /// <param name="shouldUpdate">Indicates if it is an update operation</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Operation result</returns>
        Task<Result>  PushNodes<T>(
            UAExtractor extractor,
            string database,
            string table,
            IDictionary<string, T> rows,
            ConverterType converter,
            bool shouldUpdate,
            CancellationToken token
        )
            where T : BaseUANode;

        /// <summary>
        /// Updates all BaseUANode to CDF raw
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="database">Name of metadata database in CDF</param>
        /// <param name="table">Name of metadata table in CDF</param>
        /// <param name="dataSet">Dictionary map of BaseUANode of their keys</param>
        /// <param name="converter">Converter</param>
        /// <param name="result">Operation result</param>
        /// <param name="shouldUpdate">Indicates if it is an update operation</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        Task<Result> PushReferences(
            string database,
            string table,
            IEnumerable<RelationshipCreate> relationships,
            CancellationToken token
        );
    }
}
