using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public class ConfigToolTestFixture : BaseExtractorTestFixture
    {
        public ConfigToolTestFixture() : base(63400)
        {
            Config.Source.PublishingInterval = 200;
            Config.Extraction.DataPushDelay = 200;
            Config.History.Enabled = false;
            Config.Events.Enabled = true;
            Config.Extraction.RootNode = Server.Ids.Event.Root.ToProtoNodeId(Client);
        }
    }
    public class ConfigToolTest : MakeConsoleWork, IClassFixture<ConfigToolTestFixture>
    {
        private readonly ConfigToolTestFixture tester;
        public ConfigToolTest(ITestOutputHelper output, ConfigToolTestFixture tester) : base(output)
        {
            this.tester = tester;
        }

    }
}
