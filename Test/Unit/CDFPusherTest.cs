using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.Pushers;
using Xunit;

namespace Test.Unit
{
    public sealed class CDFPusherTestFixture : BaseExtractorTestFixture
    {
        public CDFPusherTestFixture() : base(62900)
        {
        }
        public (CDFMockHandler, CDFPusher) GetPusher()
        {
            var handler = new CDFMockHandler("test", CDFMockHandler.MockMode.None);
            CommonTestUtils.AddDummyProvider(handler, Services);
            Services.AddCogniteClient("appid", true, true, false);
            var provider = Services.BuildServiceProvider();
            var pusher = Config.Cognite.ToPusher(provider) as CDFPusher;
            return (handler, pusher);
        }
    }
    public class CDFPusherTest : MakeConsoleWork, IClassFixture<CDFPusherTestFixture>
    {
        private CDFPusherTestFixture tester;
        public CDFPusherTest(ITestOutputHelper output, CDFPusherTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        [Fact]
        public async Task TestTestConnection()
        {
            var (handler, pusher) = tester.GetPusher();

            handler.AllowConnectionTest = false;

            tester.Config.Cognite.Debug = true;
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
            tester.Config.Cognite.Debug = false;

            Assert.False(await pusher.TestConnection(tester.Config, tester.Source.Token));

            handler.AllowConnectionTest = true;

            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));

            handler.FailedRoutes.Add("/timeseries/list");

            Assert.False(await pusher.TestConnection(tester.Config, tester.Source.Token));

            handler.FailedRoutes.Clear();
            handler.FailedRoutes.Add("/events/list");

            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));

            tester.Config.Events.Enabled = true;

            Assert.False(await pusher.TestConnection(tester.Config, tester.Source.Token));

            handler.FailedRoutes.Clear();
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));

            tester.Config.Events.Enabled = false;
        }
    }
}
