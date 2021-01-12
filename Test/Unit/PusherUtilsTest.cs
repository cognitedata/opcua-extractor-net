using Cognite.Extractor.Common;
using Cognite.OpcUa.Pushers;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class PusherTestFixture : BaseExtractorTestFixture
    {
        public PusherTestFixture() : base(62800) { }
    }
    public class PusherUtilsTest : MakeConsoleWork, IClassFixture<PusherTestFixture>
    {
        private PusherTestFixture tester;
        public PusherUtilsTest(ITestOutputHelper output, PusherTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        [Fact]
        public void TestGetTimestampValue()
        {
            var ts = DateTime.UtcNow;
            var result = ts.ToUnixTimeMilliseconds();
            Assert.Equal(result, PusherUtils.GetTimestampValue(ts));
            Assert.Equal(result, PusherUtils.GetTimestampValue(result));
            Assert.Equal(result, PusherUtils.GetTimestampValue(result.ToString(CultureInfo.InvariantCulture)));
            Assert.Equal(0, PusherUtils.GetTimestampValue("Hey there"));
        }
    }
}
