using Cognite.Extractor.Common;
using Cognite.OpcUa;
using System.Threading;
using System.Threading.Tasks;

namespace Test.Utils
{
    public class DummyClientCallbacks : IClientCallbacks
    {
        public PeriodicScheduler TaskScheduler { get; }
        public bool Connected { get; set; }
        public int ServiceLevelCbCount { get; set; }
        public int LowServiceLevelCbCount { get; set; }

        public DummyClientCallbacks(CancellationToken token)
        {
            TaskScheduler = new PeriodicScheduler(token);
        }

        public Task OnServerDisconnect(UAClient source)
        {
            Connected = false;
            return Task.CompletedTask;
        }

        public Task OnServerReconnect(UAClient source)
        {
            Connected = true;
            return Task.CompletedTask;
        }

        public Task OnServiceLevelAboveThreshold(UAClient source)
        {
            ServiceLevelCbCount++;
            return Task.CompletedTask;
        }

        public Task OnServicelevelBelowThreshold(UAClient source)
        {
            LowServiceLevelCbCount++;
            return Task.CompletedTask;
        }
    }
}
