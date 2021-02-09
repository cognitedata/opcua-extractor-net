/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Cognite.Extractor.Configuration;
using Cognite.OpcUa;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [Collection("Extractor tests")]
    public class EventTests : MakeConsoleWork
    {
        public EventTests(ITestOutputHelper output) : base(output) { }

        [Fact]
        [Trait("Server", "audit")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "audit")]
        public async Task TestAuditEvents()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Audit
            });
            await tester.ClearPersistentData();

            tester.Config.Extraction.EnableAuditDiscovery = true;

            await tester.StartServer();

            tester.StartExtractor();

            await tester.Extractor.Looper.WaitForNextPush();
            Assert.Equal(4, tester.Handler.Assets.Count);
            Assert.Empty(tester.Handler.Timeseries);

            int assetCount = tester.Handler.Assets.Count;
            int tsCount = tester.Handler.Timeseries.Count;

            tester.Server.DirectGrowth();
            await tester.WaitForCondition(() =>
                    tester.Handler.Assets.Count == 5 && tester.Handler.Timeseries.Count == 1,
                20, "Expected directly added timeseries and assets to be discovered");

            var directRoot = tester.Handler.Assets.Values.FirstOrDefault(asset => asset.name == "AddDirect");

            var directAsset = tester.Handler.Assets.Values.FirstOrDefault(asset => asset.name == "AddObj 0");
            var directTs = tester.Handler.Timeseries.Values.FirstOrDefault(ts => ts.name == "AddVar 0");

            Assert.NotNull(directAsset);
            Assert.NotNull(directTs);
            Assert.Equal(directRoot.id, directTs.assetId);
            Assert.Equal(directRoot.externalId, directAsset.parentExternalId);

            tester.Server.ReferenceGrowth(1);
            await tester.WaitForCondition(() =>
                tester.Handler.Assets.Count == 6 && tester.Handler.Timeseries.Count == 2,
                20, "Expected reference added timeseries and assets to be discovered");

            var refRoot = tester.Handler.Assets.Values.FirstOrDefault(asset => asset.name == "AddRef");

            var refAsset = tester.Handler.Assets.Values.FirstOrDefault(asset => asset.name == "AddObj 1");
            var refTs = tester.Handler.Timeseries.Values.FirstOrDefault(ts => ts.name == "AddVar 1");

            Assert.NotNull(refAsset);
            Assert.NotNull(refTs);
            Assert.Equal(refRoot.id, refTs.assetId);
            Assert.Equal(refRoot.externalId, refAsset.parentExternalId);

            await tester.TerminateRunTask(false);
        }
        [Fact]
        [Trait("Server", "events")]
        [Trait("Target", "FailureBuffer")]
        [Trait("Test", "influxeventsbuffering")]
        public async Task TestEventsInfluxBuffering()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ConfigName = ConfigName.Events,
                ServerName = ServerName.Events,
                FailureInflux = true
            });
            await tester.ClearPersistentData();

            tester.Config.History.Enabled = true;

            tester.Handler.AllowEvents = false;
            tester.Handler.AllowPush = false;
            tester.Handler.AllowConnectionTest = false;

            await tester.StartServer();
            tester.Server.PopulateEvents();

            tester.StartExtractor();
            await tester.Extractor.WaitForSubscriptions();
            await tester.WaitForCondition(() => tester.Pusher.EventsFailing, 20, "Expect pusher to start failing");

            tester.Server.TriggerEvents(100);
            tester.Server.TriggerEvents(101);

            await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.AnyEvents
                && tester.Pusher.EventsFailing,
                20, "Expected failurebuffer to contain some events");
            await tester.Extractor.Looper.WaitForNextPush();

            tester.Handler.AllowEvents = true;
            tester.Handler.AllowPush = true;
            tester.Handler.AllowConnectionTest = true;
            await tester.WaitForCondition(() => !tester.Extractor.FailureBuffer.AnyEvents,
                20, "Expected FailureBuffer to be emptied");

            Assert.False(tester.Extractor.FailureBuffer.AnyEvents);

            await tester.WaitForCondition(() => tester.Handler.Events.Count == 920, 10,
                () => $"Expected to receive 920 events, but got {tester.Handler.Events.Count}");

            await tester.TerminateRunTask(true);

            var events = tester.Handler.Events.Values.ToList();

            foreach (var ev in events)
            {
                CommonTestUtils.TestEvent(ev, tester.Handler);
            }
        }
    }
}
