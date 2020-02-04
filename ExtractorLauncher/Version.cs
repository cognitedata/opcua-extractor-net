using System.IO;
using System.Text.RegularExpressions;

namespace Cognite.OpcUa
{
    public static class Version
    {
        private static string Read(string property)
        {
            var assembly = System.Reflection.Assembly.GetExecutingAssembly();
            using var stream = assembly.GetManifestResourceStream($"Cognite.OpcUa.Properties.{property}");
            if (stream == null) return null;
            using var reader = new StreamReader(stream);
            return reader.ReadToEnd().Trim();
        }

        public static string Status()
        {
            string hash = Read("GitCommitHash");
            string time = Read("GitCommitTime");
            return $"{hash} {time}";
        }
        public static string GetVersion()
        {
            string raw = Read("GitCommitHash");
            Regex rgx = new Regex(@"-(\d+)-*.*");
            return rgx.Replace(raw, "-pre.$1", 1);
        }
    }
}
