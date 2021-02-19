using System;
using System.Diagnostics;
using System.IO;

namespace OpcUaServiceManager
{
    /// <summary>
    /// Runs a command
    /// </summary>
    class RunCommand
    {
        /// <summary>
        /// Runs a command in cmd.exe - timeout will occur after 1 minute
        /// </summary>
        /// <param name="argument"></param>
        /// <returns>stdout of the command</returns>
        public static string Run(string argument)
        {
            ProcessStartInfo pinfo = new ProcessStartInfo();
            pinfo.FileName = "cmd.exe";
            pinfo.Arguments = argument;
            pinfo.UseShellExecute = false;
            pinfo.RedirectStandardOutput = true;
            pinfo.CreateNoWindow = true;
            string result = string.Empty;

            try
            {
                using (Process exeProcess = Process.Start(pinfo))
                {
                    using (StreamReader reader = exeProcess.StandardOutput)
                    {
                        result = reader.ReadToEnd();
                    }
                    // Wait 1 minute for the command to finish
                    exeProcess.WaitForExit(1000 * 60 * 1);
                }
            }
            catch (Exception ex)
            {
                return ex.ToString();
            }
            return result;

        }
    }
}
