using System;
using System.Collections.Generic;
using System.Text;
using Xunit.Abstractions;

namespace Test
{
    /// <summary>
    /// Tests for the full MQTT pipeline to CDF mocker.
    /// </summary>
    public class MQTTPusherTests : MakeConsoleWork
    {
        public MQTTPusherTests(ITestOutputHelper output) : base(output) { }


    }
}
