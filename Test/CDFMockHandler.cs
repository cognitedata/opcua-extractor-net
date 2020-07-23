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
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Com.Cognite.V1.Timeseries.Proto;
using Google.Protobuf;
using Newtonsoft.Json;
using Serilog;
using Xunit;

#pragma warning disable IDE1006 // Naming Styles

namespace Test
{
    public class CDFMockHandler
    {
        readonly object handlerLock = new object();
        readonly string project;
        public Dictionary<string, AssetDummy> Assets { get; } = new Dictionary<string, AssetDummy>();
        public Dictionary<string, TimeseriesDummy> Timeseries { get; } = new Dictionary<string, TimeseriesDummy>();
        public Dictionary<string, EventDummy> Events { get; } = new Dictionary<string, EventDummy>();
        public Dictionary<string, (List<NumericDatapoint> NumericDatapoints, List<StringDatapoint> StringDatapoints)> Datapoints { get; } =
            new Dictionary<string, (List<NumericDatapoint> NumericDatapoints, List<StringDatapoint> StringDatapoints)>();

        public Dictionary<string, AssetDummy> AssetRaw { get; } = new Dictionary<string, AssetDummy>();
        public Dictionary<string, StatelessTimeseriesDummy> TimeseriesRaw { get; } = new Dictionary<string, StatelessTimeseriesDummy>();

        long assetIdCounter = 1;
        long timeseriesIdCounter = 1;
        long eventIdCounter = 1;
        private long requestIdCounter = 1;
        public long RequestCount { get; private set; }
        public bool BlockAllConnections { get; set; } = false;
        public bool AllowPush { get; set; } = true;
        public bool AllowEvents { get; set; } = true;
        public bool AllowConnectionTest { get; set; } = true;
        public bool StoreDatapoints { get; set; } = false;
        public MockMode mode { get; set; }

        private readonly ILogger log = Log.Logger.ForContext(typeof(CDFMockHandler));

        public enum MockMode
        {
            None, Some, All, FailAsset
        }
        public CDFMockHandler(string project, MockMode mode)
        {
            this.project = project;
            this.mode = mode;
        }

        public HttpMessageHandler GetHandler()
        {
            return new HttpMessageHandlerStub(MessageHandler);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Messages should be disposed in the client")]
        private async Task<HttpResponseMessage> MessageHandler(HttpRequestMessage req, CancellationToken cancellationToken)
        {
            RequestCount++;

            if (BlockAllConnections)
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

            if (req.RequestUri.AbsolutePath == "/login/status")
            {
                return HandleLoginStatus();
            }
            string reqPath = req.RequestUri.AbsolutePath.Replace($"/api/v1/projects/{project}", "", StringComparison.InvariantCulture);

            if (reqPath == "/timeseries/data" && req.Method == HttpMethod.Post && StoreDatapoints)
            {
                var proto = await req.Content.ReadAsByteArrayAsync();
                var data = DataPointInsertionRequest.Parser.ParseFrom(proto);
                return HandleTimeseriesData(data);
            }

            string content = "";
            try
            {
                content = await req.Content.ReadAsStringAsync();
            }
            catch { }
            lock (handlerLock)
            {
                HttpResponseMessage res;
                try
                {
                    switch (reqPath)
                    {
                        case "/assets/byids":
                            res = HandleAssetsByIds(content);
                            break;
                        case "/assets":
                            res = HandleCreateAssets(content);
                            break;
                        case "/assets/update":
                            res = HandleUpdateAssets(content);
                            break;
                        case "/timeseries/byids":
                            res = HandleGetTimeseries(content);
                            break;
                        case "/timeseries":
                            res = req.Method == HttpMethod.Get
                                ? HandleListTimeseries()
                                : HandleCreateTimeseries(content);
                            break;
                        case "/timeseries/list":
                            res = HandleListTimeseries();
                            break;
                        case "/timeseries/data":
                            res = HandleTimeseriesData(null);
                            break;
                        case "/timeseries/data/latest":
                            res = HandleGetTimeseries(content);
                            break;
                        case "/timeseries/data/list":
                            res = HandleGetDatapoints(content);
                            break;
                        case "/timeseries/update":
                            res = HandleUpdateTimeSeries(content);
                            break;
                        case "/events/list":
                            res = HandleListEvents();
                            break;
                        case "/events":
                            res = req.Method == HttpMethod.Get
                                ? HandleListEvents()
                                : HandleCreateEvents(content);
                            break;
                        case "/raw/dbs/metadata/tables/assets/rows":
                            res = req.Method == HttpMethod.Get
                                ? HandleGetRawAssets()
                                : HandleCreateRawAssets(content);
                            break;
                        case "/raw/dbs/metadata/tables/timeseries/rows":
                            res = req.Method == HttpMethod.Get
                                ? HandleGetRawTimeseries()
                                : HandleCreateRawTimeseries(content);
                            break;
                        default:
                            log.Warning("Unknown path: {DummyFactoryUnknownPath}", reqPath);
                            res = new HttpResponseMessage(HttpStatusCode.InternalServerError);
                            break;
                    }
                }
                catch (Exception e)
                {
                    log.Error(e, "Error in mock handler");
                    res = new HttpResponseMessage(HttpStatusCode.BadRequest);
                }
                res.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                res.Headers.Add("x-request-id", (requestIdCounter++).ToString(CultureInfo.InvariantCulture));
                /*var contentTask = res.Content.ReadAsStringAsync();
                contentTask.Wait();
                log.Information(contentTask.Result);*/
                return res;
            }
        }

        private HttpResponseMessage HandleAssetsByIds(string content)
        {
            var ids = JsonConvert.DeserializeObject<IdentityListWrapper>(content);
            var found = new List<string>();
            var missing = new List<string>();
            foreach (var id in ids.items)
            {
                if (Assets.ContainsKey(id.externalId))
                {
                    found.Add(id.externalId);
                }
                else
                {
                    missing.Add(id.externalId);
                }
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
            if (missing.Any() && !ids.ignoreUnknownIds)
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
                        items = found.Concat(missing).Select(aid => Assets[aid])
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
                    items = found.Select(id => Assets[id])
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
                asset.createdTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
                asset.lastUpdatedTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
                asset.rootId = 123;
                Assets.Add(asset.externalId, asset);
            }
            string result = JsonConvert.SerializeObject(newAssets);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(result)
            };
        }

        private static HttpResponseMessage HandleListEvents()
        {
            string res = JsonConvert.SerializeObject(new EventsReadWrapper
            {
                items = new List<EventDummy>()
            });
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(res)
            };
        }

        private static HttpResponseMessage HandleListTimeseries()
        {
            string res = JsonConvert.SerializeObject(new TimeseriesReadWrapper
            {
                items = new List<TimeseriesDummy>()
            });
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(res)
            };
        }

        private HttpResponseMessage HandleGetTimeseries(string content)
        {
            var ids = JsonConvert.DeserializeObject<IdentityListWrapper>(content);
            var found = new List<string>();
            var missing = new List<string>();
            foreach (var id in ids.items)
            {
                if (Timeseries.ContainsKey(id.externalId))
                {
                    found.Add(id.externalId);
                }
                else
                {
                    missing.Add(id.externalId);
                }
            }
            if (missing.Any() && !ids.ignoreUnknownIds)
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
                        items = found.Concat(missing).Select(tid => Timeseries[tid])
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
                    items = found.Select(id => Timeseries[id])
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
                ts.createdTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
                ts.lastUpdatedTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
                Timeseries.Add(ts.externalId, ts);
            }
            string result = JsonConvert.SerializeObject(newTimeseries);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(result)
            };
        }

        private HttpResponseMessage HandleTimeseriesData(DataPointInsertionRequest req)
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

            if (req == null)
            {
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("{}")
                };
            }

            foreach (var item in req.Items)
            {
                if (!Datapoints.ContainsKey(item.ExternalId))
                {
                    Datapoints[item.ExternalId] = (new List<NumericDatapoint>(), new List<StringDatapoint>());
                }
                if (item.DatapointTypeCase == DataPointInsertionItem.DatapointTypeOneofCase.NumericDatapoints)
                {
                    log.Information("{cnt} numeric datapoints to {id}", item.NumericDatapoints.Datapoints.Count, item.ExternalId);
                    Datapoints[item.ExternalId].NumericDatapoints.AddRange(item.NumericDatapoints.Datapoints);
                }
                else
                {
                    log.Information("{cnt} string datapoints to {id}", item.StringDatapoints.Datapoints.Count, item.ExternalId);
                    Datapoints[item.ExternalId].StringDatapoints.AddRange(item.StringDatapoints.Datapoints);
                }
            }

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{}")
            };
        }

        private HttpResponseMessage HandleCreateEvents(string content)
        {
            if (!AllowEvents)
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
            var newEvents = JsonConvert.DeserializeObject<EventWrapper>(content);
            var duplicated = new List<Identity>();
            var created = new List<(string Id, EventDummy Event)>();
            foreach (var ev in newEvents.items)
            {
                if (Events.ContainsKey(ev.externalId) || created.Any(ct => ct.Id == ev.externalId))
                {
                    duplicated.Add(new Identity { externalId = ev.externalId });
                    continue;
                }
                if (duplicated.Any()) continue;
                ev.id = eventIdCounter++;
                ev.createdTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
                ev.lastUpdatedTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
                created.Add((ev.externalId, ev));
            }

            if (duplicated.Any())
            {
                string errResult = JsonConvert.SerializeObject(new ErrorWrapper
                {
                    error = new ErrorContent
                    {
                        duplicated = duplicated,
                        code = 409,
                        message = "duplicated"
                    }
                });
                return new HttpResponseMessage(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent(errResult)
                };
            }
            foreach ((string id, var eventDummy) in created)
            {
                Events.Add(id, eventDummy);
            }
            string result = JsonConvert.SerializeObject(newEvents);
            return new HttpResponseMessage(HttpStatusCode.Created)
            {
                Content = new StringContent(result)
            };
        }

        private HttpResponseMessage HandleLoginStatus()
        {
            if (!AllowConnectionTest)
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
            var status = new LoginInfo
            {
                apiKeyId = 1,
                loggedIn = true,
                project = project,
                projectId = 1,
                user = "user"
            };
            var result = new LoginInfoWrapper
            {
                data = status
            };
            var data = JsonConvert.SerializeObject(result);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(data)
            };
        }

        private HttpResponseMessage HandleGetDatapoints(string content)
        {
            // Ignore query for now, this is only used to read the first point, so we just respond with that.
            var ids = JsonConvert.DeserializeObject<IdentityListWrapper>(content);
            var ret = new DataPointListResponse();
            var missing = new List<Identity>();
            foreach (var id in ids.items)
            {
                if (!Timeseries.ContainsKey(id.externalId))
                {
                    missing.Add(id);
                    continue;
                }
                var item = new DataPointListItem
                {
                    ExternalId = id.externalId,
                    Id = Timeseries[id.externalId].id
                };
                if (Datapoints.ContainsKey(id.externalId))
                {
                    if (Timeseries[id.externalId].isString)
                    {
                        item.StringDatapoints = new StringDatapoints();
                        item.StringDatapoints.Datapoints.Add(Datapoints[id.externalId].StringDatapoints.Aggregate((curMin, x) =>
                            curMin == null || x.Timestamp < curMin.Timestamp ? x : curMin));
                    }
                    else
                    {
                        item.NumericDatapoints = new NumericDatapoints();
                        item.NumericDatapoints.Datapoints.Add(Datapoints[id.externalId].NumericDatapoints.Aggregate((curMin, x) =>
                            curMin == null || x.Timestamp < curMin.Timestamp ? x : curMin));
                    }
                }

                ret.Items.Add(item);
            }

            var respContent = new ByteArrayContent(ret.ToByteArray());
            respContent.Headers.Add("Content-Type", "application/protobuf");
            var res = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = respContent
            };
            return res;
        }
        private HttpResponseMessage HandleGetRawAssets()
        {
            var data = new RawListWrapper<AssetDummy>();
            data.items = AssetRaw.Select(kvp => new RawWrapper<AssetDummy> { columns = kvp.Value, key = kvp.Key, lastUpdatedTime = 0 });
            var content = JsonConvert.SerializeObject(data);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(content)
            };
        }
        private HttpResponseMessage HandleGetRawTimeseries()
        {
            var data = new RawListWrapper<StatelessTimeseriesDummy>();
            data.items = TimeseriesRaw.Select(kvp => new RawWrapper<StatelessTimeseriesDummy> { columns = kvp.Value, key = kvp.Key, lastUpdatedTime = 0 });
            var content = JsonConvert.SerializeObject(data);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(content)
            };
        }
        private HttpResponseMessage HandleCreateRawAssets(string content)
        {
            var toCreate = JsonConvert.DeserializeObject<RawListWrapper<AssetDummy>>(content);
            foreach (var item in toCreate.items)
            {
                AssetRaw[item.key] = item.columns;
            }

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{}")
            };
        }
        private HttpResponseMessage HandleCreateRawTimeseries(string content)
        {
            log.Information(content);
            var toCreate = JsonConvert.DeserializeObject<RawListWrapper<StatelessTimeseriesDummy>>(content);
            foreach (var item in toCreate.items)
            {
                TimeseriesRaw[item.key] = item.columns;
            }
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{}")
            };
        }

        private HttpResponseMessage HandleUpdateAssets(string content)
        {
            var toUpdate = JsonConvert.DeserializeObject<ItemWrapper<AssetUpdateItem>>(content);
            var ret = new List<AssetDummy>();
            var missing = toUpdate.Items.Where(upd => !Assets.ContainsKey(upd.externalId))
                .Select(upd => upd.externalId);

            if (missing.Any())
            {
                var missingContent = new ErrorWrapper
                {
                    error = new ErrorContent
                    {
                        missing = missing.Select(id => new Identity { externalId = id }),
                        message = "missing",
                        code = 400
                    }
                };
                return new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    Content = new StringContent(JsonConvert.SerializeObject(missingContent))
                };
            }

            foreach (var item in toUpdate.Items)
            {
                var old = Assets[item.externalId];
                var upd = item.update;
                if (upd.parentExternalId != null)
                {
                    old.parentExternalId = upd.parentExternalId.set;
                }
                if (upd.name != null)
                {
                    old.name = upd.name.set;
                }
                if (upd.description != null)
                {
                    old.description = upd.description.set;
                }
                if (upd.metadata != null)
                {
                    if (upd.metadata.set != null)
                    {
                        old.metadata = upd.metadata.set;
                    }
                    else if (old.metadata == null)
                    {
                        old.metadata = new Dictionary<string, string>();
                    }
                    if (upd.metadata.remove != null)
                    {
                        foreach (var field in upd.metadata.remove)
                        {
                            old.metadata.Remove(field);
                        }
                    }
                    if (upd.metadata.add != null)
                    {
                        foreach (var field in upd.metadata.add)
                        {
                            old.metadata[field.Key] = field.Value;
                        }
                    }
                }
                ret.Add(old);
            }
            var result = new AssetReadWrapper
            {
                items = ret,
            };
            return new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(JsonConvert.SerializeObject(result))
            };
        }

        private HttpResponseMessage HandleUpdateTimeSeries(string content)
        {
            var toUpdate = JsonConvert.DeserializeObject<ItemWrapper<TimeseriesUpdateItem>>(content);
            var ret = new List<TimeseriesDummy>();
            var missing = toUpdate.Items.Where(upd => !Timeseries.ContainsKey(upd.externalId))
                .Select(upd => upd.externalId);

            if (missing.Any())
            {
                var missingContent = new ErrorWrapper
                {
                    error = new ErrorContent
                    {
                        missing = missing.Select(id => new Identity { externalId = id }),
                        message = "missing",
                        code = 400
                    }
                };
                return new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    Content = new StringContent(JsonConvert.SerializeObject(missingContent))
                };
            }

            foreach (var item in toUpdate.Items)
            {
                var old = Timeseries[item.externalId];
                var upd = item.update;
                if (upd.assetId != null)
                {
                    old.assetId = upd.assetId.set;
                }
                if (upd.name != null)
                {
                    old.name = upd.name.set;
                }
                if (upd.description != null)
                {
                    old.description = upd.description.set;
                }
                if (upd.metadata != null)
                {
                    if (upd.metadata.set != null)
                    {
                        old.metadata = upd.metadata.set;
                    }
                    else if (old.metadata == null)
                    {
                        old.metadata = new Dictionary<string, string>();
                    }
                    if (upd.metadata.remove != null)
                    {
                        foreach (var field in upd.metadata.remove)
                        {
                            old.metadata.Remove(field);
                        }
                    }
                    if (upd.metadata.add != null)
                    {
                        foreach (var field in upd.metadata.add)
                        {
                            old.metadata[field.Key] = field.Value;
                        }
                    }
                }
                ret.Add(old);
            }
            var result = new TimeseriesReadWrapper
            {
                items = ret,
            };
            return new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(JsonConvert.SerializeObject(result))
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
                createdTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds(),
                lastUpdatedTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds(),
                rootId = 123
            };
            Assets.Add(externalId, asset);
            return asset;
        }

        private TimeseriesDummy MockTimeseries(string externalId)
        {
            var ts = new TimeseriesDummy(mode)
            {
                id = timeseriesIdCounter++,
                isString = false,
                isStep = false,
                createdTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds(),
                externalId = externalId
            };
            Timeseries.Add(externalId, ts);
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
        public string parentExternalId { get; set; }
    }
    public class AssetReadWrapper
    {
        public IEnumerable<AssetDummy> items { get; set; }
    }

    public class NullableSet<T>
    {
        public T set { get; set; }
        public bool setNull { get; set; }
    }

    public class DictionaryUpdate
    {
        public Dictionary<string, string> set { get; set; }
        public IEnumerable<string> remove { get; set; }
        public Dictionary<string, string> add { get; set; }
    }
    public class AssetUpdate
    {
        public DictionaryUpdate metadata { get; set; }
        public NullableSet<string> parentExternalId { get; set; }
        public NullableSet<string> name { get; set; }
        public NullableSet<string> description { get; set; }
    }
    public class AssetUpdateItem
    {
        public string externalId { get; set; }
        public AssetUpdate update { get; set; }
    }

    public class TimeseriesUpdate
    {
        public DictionaryUpdate metadata { get; set; }
        public NullableSet<long> assetId { get; set; }
        public NullableSet<string> name { get; set; }
        public NullableSet<string> description { get; set; }
    }
    public class TimeseriesUpdateItem
    {
        public string externalId { get; set; }
        public TimeseriesUpdate update { get; set; }
    }
    public class Identity
    {
        public string externalId { get; set; }
    }
    public class IdentityListWrapper
    {
        public IEnumerable<Identity> items { get; set; }
        public bool ignoreUnknownIds { get; set; }
    }
    public class ErrorContent
    {
        public int code { get; set; }
        public string message { get; set; }
        public IEnumerable<Identity> missing { get; set; }
        public IEnumerable<Identity> duplicated { get; set; } = new List<Identity>();
    }
    public class ErrorWrapper
    {
        public ErrorContent error { get; set; }
    }
    public class TimeseriesReadWrapper
    {
        public IEnumerable<TimeseriesDummy> items { get; set; }
    }
    public class EventsReadWrapper
    {
        public IEnumerable<EventDummy> items { get; set; }
    }
    public class TimeseriesDummy
    {
        public TimeseriesDummy() : this(CDFMockHandler.MockMode.None) { }
        public TimeseriesDummy(CDFMockHandler.MockMode mode)
        {
            if (mode == CDFMockHandler.MockMode.None || mode == CDFMockHandler.MockMode.FailAsset)
            {
                datapoints = new List<DataPoint>();
            }
            else
            {
                datapoints = new List<DataPoint>
                {
                    new DataPoint
                    {
                        timestamp = new DateTimeOffset(DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(15))).ToUnixTimeMilliseconds(),
                        value = 0
                    }
                };
            }
        }
        public long id { get; set; }
        public bool isString { get; set; }
        public bool isStep { get; set; }
        public long createdTime { get; set; }
        public long lastUpdatedTime { get; set; }
        public Dictionary<string, string> metadata { get; set; }
        public string externalId { get; set; }
        public IEnumerable<DataPoint> datapoints { get; set; }
        public string name { get; set; }
        public string description { get; set; }
        public long? assetId { get; set; }
        public string unit { get; set; }
    }
    public class StatelessTimeseriesDummy : TimeseriesDummy
    {
        public string assetExternalId { get; set; }
    }
    public class RawWrapper<T>
    {
        public string key { get; set; }
        public long lastUpdatedTime { get; set; }
        public T columns { get; set; }
    }

    public class RawListWrapper<T>
    {
        public IEnumerable<RawWrapper<T>> items { get; set; }
    }


    public class LoginInfo
    {
        public string user { get; set; }
        public bool loggedIn { get; set; }
        public string project { get; set; }
        public long projectId { get; set; }
        public long apiKeyId { get; set; }
    }

    public class LoginInfoWrapper
    {
        public LoginInfo data { get; set; }
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
    public class ItemWrapper<T>
    {
        public IEnumerable<T> Items { get; set; }
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
