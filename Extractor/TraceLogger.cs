using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace Cognite.OpcUa
{
    internal static class TraceLogger
    {
        public static void LogDump<T>(this ILogger log, string message, T item)
        {
            string res;
            try
            {
                res = JsonSerializer.Serialize(item, new JsonSerializerOptions
                {
                    WriteIndented = true,
                    MaxDepth = 10
                });
            }
            catch
            {
                res = item?.ToString() ?? "";
            }

            log.LogTrace("TRACE: {Message} {Res}", message, res);
        }
    }
}
