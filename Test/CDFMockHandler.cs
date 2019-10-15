/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

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
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Serilog;
#pragma warning disable IDE1006 // Naming Styles

namespace Test
{
    public class CDFMockHandler
    {
        readonly object handlerLock = new object();
        readonly string project;
        public readonly Dictionary<string, AssetDummy> assets = new Dictionary<string, AssetDummy>();
        public readonly Dictionary<string, TimeseriesDummy> timeseries = new Dictionary<string, TimeseriesDummy>();
        public readonly Dictionary<string, EventDummy> events = new Dictionary<string, EventDummy>();
        long assetIdCounter = 1;
        long timeseriesIdCounter = 1;
        long eventIdCounter = 1;
        public long RequestCount { get; private set; }
        public bool AllowPush { get; set; } = true;
        public MockMode mode;
        public enum MockMode
        {
            None, Some, All, FailAsset
        }
        public CDFMockHandler(string project, MockMode mode)
        {
            this.project = project;
            this.mode = mode;
        }

        public HttpMessageHandler GetHandler(string name = "client")
        {
            return new HttpMessageHandlerStub(MessageHandler);
        }

        private async Task<HttpResponseMessage> MessageHandler(HttpRequestMessage req, CancellationToken cancellationToken)
        {
            string reqPath = req.RequestUri.AbsolutePath.Replace($"/api/v1/projects/{project}", "");
            string content = await req.Content.ReadAsStringAsync();
            lock (handlerLock)
            {
                RequestCount++;
                switch (reqPath)
                {
                    case "/assets/byids":
                        return HandleAssetsByIds(content);
                    case "/assets":
                        return HandleCreateAssets(content);
                    case "/timeseries/byids":
                        return HandleGetTimeseries(content);
                    case "/timeseries":
                        return HandleCreateTimeseries(content);
                    case "/timeseries/data":
                        return HandleTimeseriesData();
                    case "/timeseries/data/latest":
                        return HandleGetTimeseries(content);
                    case "/events":
                        return HandleCreateEvents(content);
                    default:
                        Log.Warning("Unknown path: {DummyFactoryUnknownPath}", reqPath);
                        return new HttpResponseMessage(HttpStatusCode.InternalServerError);
                }
            }
        }

        private HttpResponseMessage HandleAssetsByIds(string content)
        {
            var ids = JsonConvert.DeserializeObject<IdentityListWrapper>(content);
            var found = new List<string>();
            var missing = new List<string>();
            foreach (var id in ids.items)
            {
                if (assets.ContainsKey(id.externalId))
                {
                    found.Add(id.externalId);
                }
                else
                {
                    missing.Add(id.externalId);
                }
            }
            if (missing.Any())
            {
                IList<string> finalMissing;
                if (mode == MockMode.All)
                {
                    foreach (string id in missing)
                    {
                        MockAsset(id);
                    }
                    string res = JsonConvert.SerializeObject(new AssetReadWrapper
                    {
                        items = found.Concat(missing).Select(aid => assets[aid])
                    });
                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(res)
                    };
                }
                if (mode == MockMode.FailAsset)
                {
                    string res = JsonConvert.SerializeObject(new ErrorWrapper
                    {
                        error = new ErrorContent
                        {
                            code = 400,
                            message = "weird failure"
                        }
                    });
                    return new HttpResponseMessage(HttpStatusCode.BadRequest)
                    {
                        Content = new StringContent(res)
                    };
                }
                if (mode == MockMode.Some)
                {
                    finalMissing = new List<string>();
                    int count = 1;
                    foreach (string id in missing)
                    {
                        if (count++ % 2 == 0)
                        {
                            MockAsset(id);
                        }
                        else
                        {
                            finalMissing.Add(id);
                        }
                    }
                }
                else
                {
                    finalMissing = missing;
                }
                string result = JsonConvert.SerializeObject(new ErrorWrapper
                {
                    error = new ErrorContent
                    {
                        missing = finalMissing.Select(id => new Identity { externalId = id }),
                        code = 400,
                        message = "missing"
                    }
                });
                return new HttpResponseMessage(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent(result)
                };
            }
            else
            {
                string result = JsonConvert.SerializeObject(new AssetReadWrapper
                {
                    items = found.Select(id => assets[id])
                });
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(result)
                };
            }
        }

        private HttpResponseMessage HandleCreateAssets(string content)
        {
            var newAssets = JsonConvert.DeserializeObject<AssetReadWrapper>(content);
            foreach (var asset in newAssets.items)
            {
                asset.id = assetIdCounter++;
                asset.createdTime = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds();
                asset.lastUpdatedTime = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds();
                asset.rootId = 123;
                assets.Add(asset.externalId, asset);
            }
            string result = JsonConvert.SerializeObject(newAssets);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(result)
            };
        }

        private HttpResponseMessage HandleGetTimeseries(string content)
        {
            var ids = JsonConvert.DeserializeObject<IdentityListWrapper>(content);
            var found = new List<string>();
            var missing = new List<string>();
            foreach (var id in ids.items)
            {
                if (timeseries.ContainsKey(id.externalId))
                {
                    found.Add(id.externalId);
                }
                else
                {
                    missing.Add(id.externalId);
                }
            }
            if (missing.Any())
            {
                IList<string> finalMissing;
                if (mode == MockMode.All)
                {
                    foreach (string id in missing)
                    {
                        MockTimeseries(id);
                    }
                    string res = JsonConvert.SerializeObject(new TimeseriesReadWrapper
                    {
                        items = found.Concat(missing).Select(tid => timeseries[tid])
                    });
                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(res)
                    };
                }
                if (mode == MockMode.Some)
                {
                    finalMissing = new List<string>();
                    int count = 1;
                    foreach (string id in missing)
                    {
                        if (count++ % 2 == 0)
                        {
                            MockTimeseries(id);
                        }
                        else
                        {
                            finalMissing.Add(id);
                        }
                    }
                }
                else
                {
                    finalMissing = missing;
                }
                string result = JsonConvert.SerializeObject(new ErrorWrapper
                {
                    error = new ErrorContent
                    {
                        missing = finalMissing.Select(id => new Identity { externalId = id }),
                        code = 400,
                        message = "missing"
                    }
                });
                return new HttpResponseMessage(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent(result)
                };
            }
            else
            {
                string result = JsonConvert.SerializeObject(new TimeseriesReadWrapper
                {
                    items = found.Select(id => timeseries[id])
                });
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(result)
                };
            }
        }

        private HttpResponseMessage HandleCreateTimeseries(string content)
        {
            var newTimeseries = JsonConvert.DeserializeObject<TimeseriesReadWrapper>(content);
            foreach (var ts in newTimeseries.items)
            {
                ts.id = timeseriesIdCounter++;
                ts.createdTime = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds();
                ts.lastUpdatedTime = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds();
                timeseries.Add(ts.externalId, ts);
            }
            string result = JsonConvert.SerializeObject(newTimeseries);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(result)
            };
        }

        private HttpResponseMessage HandleTimeseriesData()
        {
            if (!AllowPush)
            {
                return new HttpResponseMessage(HttpStatusCode.InternalServerError)
                {
                    Content = new StringContent(JsonConvert.SerializeObject(new ErrorWrapper
                    {
                        error = new ErrorContent
                        {
                            code = 501,
                            message = "bad something or other"
                        }
                    }))
                };
            }
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{}")
            };
        }

        private HttpResponseMessage HandleCreateEvents(string content)
        {
            var newEvents = JsonConvert.DeserializeObject<EventWrapper>(content);
            foreach (var ev in newEvents.items)
            {
                ev.id = eventIdCounter++;
                ev.createdTime = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds();
                ev.lastUpdatedTime = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds();
                events.Add(ev.externalId, ev);
            }
            string result = JsonConvert.SerializeObject(newEvents);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(result)
            };
        }

        private AssetDummy MockAsset(string externalId)
        {
            var asset = new AssetDummy
            {
                externalId = externalId,
                name = "test",
                description = "",
                metadata = new Dictionary<string, string>(),
                id = assetIdCounter++,
                createdTime = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds(),
                lastUpdatedTime = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds(),
                rootId = 123
            };
            assets.Add(externalId, asset);
            return asset;
        }

        private TimeseriesDummy MockTimeseries(string externalId)
        {
            var ts = new TimeseriesDummy
            {
                id = timeseriesIdCounter++,
                isString = false,
                isStep = false,
                createdTime = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds(),
                externalId = externalId
            };
            timeseries.Add(externalId, ts);
            return ts;
        }
    }
    public class AssetDummy
    {
        public string externalId { get; set; }
        public string name { get; set; }
        public string description { get; set; }
        public Dictionary<string, string> metadata { get; set; }
        public long id { get; set; }
        public long createdTime { get; set; }
        public long lastUpdatedTime { get; set; }
        public long rootId { get; set; }
    }
    public class AssetReadWrapper
    {
        public IEnumerable<AssetDummy> items { get; set; }
    }
    public class Identity
    {
        public string externalId { get; set; }
    }
    public class IdentityListWrapper
    {
        public IEnumerable<Identity> items { get; set; }
    }
    public class ErrorContent
    {
        public int code { get; set; }
        public string message { get; set; }
        public IEnumerable<Identity> missing { get; set; }
    }
    public class ErrorWrapper
    {
        public ErrorContent error { get; set; }
    }
    public class TimeseriesReadWrapper
    {
        public IEnumerable<TimeseriesDummy> items { get; set; }
    }
    public class TimeseriesDummy
    {
        public TimeseriesDummy()
        {
            datapoints = new List<DataPoint>
            {
                new DataPoint
                {
                    timestamp = new DateTimeOffset(DateTime.Now.Subtract(TimeSpan.FromMinutes(15))).ToUnixTimeMilliseconds(),
                    value = 0
                }
            };
        }
        public long id { get; set; }
        public bool isString { get; set; }
        public bool isStep { get; set; }
        public long createdTime { get; set; }
        public long lastUpdatedTime { get; set; }
        public string externalId { get; set; }
        public IEnumerable<DataPoint> datapoints { get; set; }
        public string name { get; set; }
    }
    public class DataPoint
    {
        public long timestamp { get; set; }
        public double value { get; set; }
    }
    public class EventDummy
    {
        public long id { get; set; }
        public string externalId { get; set; }
        public long startTime { get; set; }
        public long endTime { get; set; }
        public string type { get; set; }
        public string subtype { get; set; }
        public string description { get; set; }
        public IDictionary<string, string> metadata { get; set; }
        public IEnumerable<long> assetIds { get; set; }
        public string source { get; set; }
        public long createdTime { get; set; }
        public long lastUpdatedTime { get; set; }
    }
    public class EventWrapper
    {
        public IEnumerable<EventDummy> items { get; set; }
    }
    public class HttpMessageHandlerStub : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _sendAsync;

        public HttpMessageHandlerStub(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> sendAsync)
        {
            _sendAsync = sendAsync;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return await _sendAsync(request, cancellationToken);
        }
    }
}
#pragma warning restore IDE1006 // Naming Styles
