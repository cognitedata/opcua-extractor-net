using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Cognite.OpcUa
{
    public static class Version
    {
        public static string Status()
        {
            return Assembly.GetExecutingAssembly().GetCustomAttribute<AssemblyDescriptionAttribute>()?.Description
                ?? "Unknown";
        }
        public static string GetVersion()
        {
            string raw = Assembly.GetExecutingAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
                .InformationalVersion ?? "1.0.0";
            Regex rgx = new Regex(@"-(\d+)-*.*");
            return rgx.Replace(raw, "-pre.$1", 1);
        }
    }
}
