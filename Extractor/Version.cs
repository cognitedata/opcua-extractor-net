using System.IO;
using System.Text.RegularExpressions;

namespace Cognite.OpcUa
{
    public class Version
    {
        private static string Read(string property)
        {
            var assembly = System.Reflection.Assembly.GetExecutingAssembly();
            using (Stream stream = assembly.GetManifestResourceStream($"Cognite.OpcUa.Properties.{property}"))
            using (StreamReader reader = new StreamReader(stream))
                return reader.ReadToEnd().Trim();
        }

        public static string Status()
        {
            var hash = Read("GitCommitHash");
            var time = Read("GitCommitTime");
            return $"{hash} {time}";
        }
        public static string GetVersion()
        {
            var raw = Read("GitCommitHash");
            Regex rgx = new Regex(@"-(\d+)-*.*");
            return rgx.Replace(raw, "-pre.$1", 1);
        }
    }
}
