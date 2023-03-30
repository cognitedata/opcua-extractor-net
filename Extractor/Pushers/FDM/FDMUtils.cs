using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Cognite.OpcUa.Pushers.FDM
{
    public static class FDMUtils
    {
        private static Regex extIdRegex = new Regex("^[a-zA-Z]([a-zA-Z0-9_]{0,253}[a-zA-Z0-9])?$", RegexOptions.Compiled);

        public static string SanitizeExternalId(string raw)
        {
            var clean = raw.Replace('-', '_').Replace(' ', '_').Replace("<", "").Replace(">", "");

            var c0 = clean[0];
            if (!(c0 >= 'a' && c0 <= 'z') && !(c0 >= 'A' && c0 <= 'Z'))
            {
                clean = $"opc{clean}";
            }

            if (!extIdRegex.IsMatch(clean))
            {
                throw new InvalidOperationException($"Invalid externalId: {clean}");
            }

            return clean;
        }
    }
}
