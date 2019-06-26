using System.Collections.Generic;
using System.Globalization;

namespace CommonUtil
{
    public class Options
    {
        public Options(string[] args)
        {
            Map = new Dictionary<string, string>();
            Set = new HashSet<string>();
            for (int i = 0; i < args.Length; ++i)
            {
                var arg = args[i];
                if (!arg.StartsWith("-"))
                {
                    continue;
                }

                if (i + 1 < args.Length && !args[i + 1].StartsWith("-"))
                {
                    Map.Add(arg.Substring(1), args[i + 1]);
                }
                else
                {
                    Set.Add(arg.Substring(1));
                }
            }
        }

        public Dictionary<string, string> Map { get; }

        public HashSet<string> Set { get; }
    }
}