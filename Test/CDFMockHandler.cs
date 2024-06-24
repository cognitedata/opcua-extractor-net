/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

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

using Cognite.OpcUa;
using CogniteSdk;
using CogniteSdk.Beta.DataModels;
using Com.Cognite.V1.Timeseries.Proto;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable IDE1006 // Naming Styles

namespace Test
{
    public class CDFMockHandler
    {
        private readonly object handlerLock = new object();
        private readonly string project;
        public Dictionary<string, AssetDummy> Assets { get; } = new Dictionary<string, AssetDummy>();
        public Dictionary<string, TimeseriesDummy> Timeseries { get; } = new Dictionary<string, TimeseriesDummy>();
        public Dictionary<string, EventDummy> Events { get; } = new Dictionary<string, EventDummy>();
        public Dictionary<string, (List<NumericDatapoint> NumericDatapoints, List<StringDatapoint> StringDatapoints)> Datapoints { get; } =
            new Dictionary<string, (List<NumericDatapoint> NumericDatapoints, List<StringDatapoint> StringDatapoints)>();

        public Dictionary<string, JsonElement> AssetsRaw { get; } = new Dictionary<string, JsonElement>();
        public Dictionary<string, JsonElement> TimeseriesRaw { get; } = new Dictionary<string, JsonElement>();
        public Dictionary<string, RelationshipDummy> Relationships { get; } = new Dictionary<string, RelationshipDummy>();
        public Dictionary<string, RelationshipDummy> RelationshipsRaw { get; } = new Dictionary<string, RelationshipDummy>();
        public Dictionary<string, DataSet> DataSets { get; } = new Dictionary<string, DataSet>();
        public Dictionary<string, Space> Spaces { get; } = new();
        public Dictionary<string, JsonObject> DataModels { get; } = new();
        public Dictionary<string, JsonObject> Views { get; } = new();
        public Dictionary<string, JsonObject> Containers { get; } = new();
        public Dictionary<string, JsonObject> Instances { get; } = new();

        private long assetIdCounter = 1;
        private long timeseriesIdCounter = 1;
        private long eventIdCounter = 1;
        private long requestIdCounter = 1;
        public long RequestCount { get; private set; }
        public bool BlockAllConnections { get; set; }
        public bool AllowPush { get; set; } = true;
        public bool AllowEvents { get; set; } = true;
        public bool StoreDatapoints { get; set; }
        public MockMode mode { get; set; }
        private HttpResponseMessage GetFailedRequest(HttpStatusCode code)
        {
            var res = new HttpResponseMessage(code)
            {
                Content = new StringContent(JsonConvert.SerializeObject(new ErrorWrapper
                {
                    error = new ErrorContent
                    {
                        code = (int)code,
                        message = code.ToString()
                    }
                }))
            };
            res.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            res.Headers.Add("x-request-id", (requestIdCounter++).ToString(CultureInfo.InvariantCulture));
            return res;
        }
        public HashSet<string> FailedRoutes { get; } = new HashSet<string>();

        private readonly ILogger log;

        public enum MockMode
        {
            None, Some, All, FailAsset
        }
        public CDFMockHandler(string project, MockMode mode, ILogger log)
        {
            this.project = project;
            this.mode = mode;
            this.log = log;
        }

        public HttpMessageHandler CreateHandler()
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
                return GetFailedRequest(HttpStatusCode.InternalServerError);
            }

            string reqPath = req.RequestUri.AbsolutePath.Replace($"/api/v1/projects/{project}", "", StringComparison.InvariantCulture);

            log.LogInformation("Request to {Path}", reqPath);
            if (FailedRoutes.Contains(reqPath))
            {
                log.LogInformation("Failing request to {Path}", reqPath);
                return GetFailedRequest(HttpStatusCode.Forbidden);
            }

            if (reqPath == "/timeseries/data" && req.Method == HttpMethod.Post && StoreDatapoints)
            {
                var proto = await req.Content.ReadAsByteArrayAsync(cancellationToken);
                var data = DataPointInsertionRequest.Parser.ParseFrom(proto);
                var res = HandleTimeseriesData(data);
                res.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                res.Headers.Add("x-request-id", (requestIdCounter++).ToString(CultureInfo.InvariantCulture));
                return res;
            }

            string content = "";
            if (req.Content != null)
            {
                try
                {
                    content = await req.Content.ReadAsStringAsync(cancellationToken);
                }
                catch { }
            }

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
                        case "/raw/dbs/metadata/tables/relationships/rows":
                            res = req.Method == HttpMethod.Get
                                ? HandleGetRawRelationships()
                                : HandleCreateRawRelationships(content);
                            break;
                        case "/relationships":
                            res = HandleCreateRelationships(content);
                            break;
                        case "/relationships/delete":
                            res = HandleDeleteRelationships(content);
                            break;
                        case "/datasets/byids":
                            res = HandleRetrieveDataSets(content);
                            break;
                        case "/models/spaces":
                            res = HandleCreateSpaces(content);
                            break;
                        case "/models/containers":
                            res = HandleCreateContainers(content);
                            break;
                        case "/models/views":
                            res = HandleCreateViews(content);
                            break;
                        case "/models/datamodels":
                            res = HandleCreateDataModels(content);
                            break;
                        case "/models/instances":
                            res = HandleCreateInstances(content);
                            break;
                        case "/models/instances/list":
                            res = HandleFilterInstances(content);
                            break;
                        case "/models/instances/delete":
                            res = HandleDeleteInstances(content);
                            break;
                        default:
                            log.LogWarning("Unknown path: {DummyFactoryUnknownPath}", reqPath);
                            res = new HttpResponseMessage(HttpStatusCode.InternalServerError);
                            break;
                    }
                }
                catch (Exception e)
                {
                    log.LogError(e, "Error in mock handler");
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
            if (missing.Count != 0 && !ids.ignoreUnknownIds)
            {
                List<string> finalMissing;
                if (mode == MockMode.All)
                {
                    foreach (string id in missing)
                    {
                        MockAsset(id);
                    }
                    string res = JsonConvert.SerializeObject(new ReadWrapper<AssetDummy>
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
                        missing = finalMissing.Select(id => new CdfIdentity { externalId = id }),
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
                string result = JsonConvert.SerializeObject(new ReadWrapper<AssetDummy>
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
            var newAssets = JsonConvert.DeserializeObject<ReadWrapper<AssetDummy>>(content);
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
            string res = JsonConvert.SerializeObject(new ReadWrapper<EventDummy>
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
            string res = JsonConvert.SerializeObject(new ReadWrapper<TimeseriesDummy>
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
            if (missing.Count != 0 && !ids.ignoreUnknownIds)
            {
                List<string> finalMissing;
                if (mode == MockMode.All)
                {
                    foreach (string id in missing)
                    {
                        MockTimeseries(id);
                    }
                    string res = JsonConvert.SerializeObject(new ReadWrapper<TimeseriesDummy>
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
                        missing = finalMissing.Select(id => new CdfIdentity { externalId = id }),
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
                var timeseries = found.Select(id => Timeseries[id]).ToList();
                foreach (var ts in timeseries)
                {
                    if (Datapoints.TryGetValue(ts.externalId, out var dps))
                    {
#pragma warning disable CA1860 // Avoid using 'Enumerable.Any()' extension method
                        if (ts.isString && (dps.StringDatapoints?.Any() ?? false))
                        {
                            ts.datapoints = new[] { new DataPoint
                            {
                                timestamp = dps.StringDatapoints.Max(dp => dp.Timestamp)
                            } };
                        }
                        else if (dps.NumericDatapoints?.Any() ?? false)
                        {
                            ts.datapoints = new[] { new DataPoint
                            {
                                timestamp = dps.NumericDatapoints.Max(dp => dp.Timestamp)
                            } };
                        }
#pragma warning restore CA1860 // Avoid using 'Enumerable.Any()' extension method
                    }
                }
                string result = JsonConvert.SerializeObject(new ReadWrapper<TimeseriesDummy>
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
            var newTimeseries = JsonConvert.DeserializeObject<ReadWrapper<TimeseriesDummy>>(content);
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

            var missing = req.Items.Where(item => !Timeseries.ContainsKey(item.ExternalId)).Select(item => item.ExternalId);
            var mismatched = req.Items.Where(item =>
            {
                if (!Timeseries.TryGetValue(item.ExternalId, out var ts)) return false;
                bool isString = item.DatapointTypeCase == DataPointInsertionItem.DatapointTypeOneofCase.StringDatapoints;
                return ts.isString != isString;
            }).Select(item => item.ExternalId);

            if (missing.Any())
            {
                return new HttpResponseMessage(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent(JsonConvert.SerializeObject(new ErrorWrapper
                    {
                        error = new ErrorContent
                        {
                            code = 400,
                            message = "missing",
                            missing = missing.Select(id => new CdfIdentity { externalId = id })
                        }
                    }))
                };
            }
            if (mismatched.Any())
            {
                return new HttpResponseMessage(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent(JsonConvert.SerializeObject(new ErrorWrapper
                    {
                        error = new ErrorContent
                        {
                            code = 400,
                            message = "Expected string value for datapoint"
                        }
                    }))
                };
            }

            foreach (var item in req.Items)
            {
                if (!Datapoints.TryGetValue(item.ExternalId, out var value))
                {
                    value = (new List<NumericDatapoint>(), new List<StringDatapoint>());
                    Datapoints[item.ExternalId] = value;
                }
                log.LogInformation("Dps to {Id}: {Num} {Str}",
                    item.ExternalId,
                    item.NumericDatapoints?.Datapoints?.Count,
                    item.StringDatapoints?.Datapoints?.Count);
                if (item.DatapointTypeCase == DataPointInsertionItem.DatapointTypeOneofCase.NumericDatapoints)
                {
                    log.LogInformation("{Count} numeric datapoints to {Id}", item.NumericDatapoints.Datapoints.Count, item.ExternalId);
                    value.NumericDatapoints.AddRange(item.NumericDatapoints.Datapoints);
                }
                else
                {
                    log.LogInformation("{Count} string datapoints to {Id}", item.StringDatapoints.Datapoints.Count, item.ExternalId);
                    value.StringDatapoints.AddRange(item.StringDatapoints.Datapoints);
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
            var duplicated = new List<CdfIdentity>();
            var created = new List<(string Id, EventDummy Event)>();
            foreach (var ev in newEvents.items)
            {
                if (Events.ContainsKey(ev.externalId) || created.Any(ct => ct.Id == ev.externalId))
                {
                    duplicated.Add(new CdfIdentity { externalId = ev.externalId });
                    continue;
                }
                if (duplicated.Count != 0) continue;
                ev.id = eventIdCounter++;
                ev.createdTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
                ev.lastUpdatedTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
                created.Add((ev.externalId, ev));
            }

            if (duplicated.Count != 0)
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

        private HttpResponseMessage HandleGetDatapoints(string content)
        {
            // Ignore query for now, this is only used to read the first point, so we just respond with that.
            var ids = JsonConvert.DeserializeObject<IdentityListWrapper>(content);
            var ret = new DataPointListResponse();
            var missing = new List<CdfIdentity>();
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
                if (Datapoints.TryGetValue(id.externalId, out var dps))
                {
                    if (Timeseries[id.externalId].isString)
                    {
                        item.StringDatapoints = new StringDatapoints();
#pragma warning disable CA1860 // Avoid using 'Enumerable.Any()' extension method
                        if (dps.StringDatapoints?.Any() ?? false)
                        {
                            item.StringDatapoints.Datapoints.Add(dps.StringDatapoints.Aggregate((curMin, x) =>
                                curMin == null || x.Timestamp < curMin.Timestamp ? x : curMin));
                        }
                    }
                    else
                    {
                        item.NumericDatapoints = new NumericDatapoints();
                        if (dps.NumericDatapoints?.Any() ?? false)
                        {
                            item.NumericDatapoints.Datapoints.Add(Datapoints[id.externalId].NumericDatapoints.Aggregate((curMin, x) =>
                                curMin == null || x.Timestamp < curMin.Timestamp ? x : curMin));
                        }
#pragma warning restore CA1860 // Avoid using 'Enumerable.Any()' extension method
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
            var data = new RawListWrapper<JsonElement>
            {
                items = AssetsRaw.Select(kvp => new RawWrapper<JsonElement> { columns = kvp.Value, key = kvp.Key, lastUpdatedTime = 0 })
            };
            var content = System.Text.Json.JsonSerializer.Serialize(data);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(content)
            };
        }
        private HttpResponseMessage HandleGetRawTimeseries()
        {
            var data = new RawListWrapper<JsonElement>
            {
                items = TimeseriesRaw.Select(kvp => new RawWrapper<JsonElement> { columns = kvp.Value, key = kvp.Key, lastUpdatedTime = 0 })
            };
            var content = System.Text.Json.JsonSerializer.Serialize(data);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(content)
            };
        }
        private HttpResponseMessage HandleGetRawRelationships()
        {
            var data = new RawListWrapper<RelationshipDummy>
            {
                items = RelationshipsRaw.Select(kvp => new RawWrapper<RelationshipDummy> { columns = kvp.Value, key = kvp.Key, lastUpdatedTime = 0 })
            };
            var content = JsonConvert.SerializeObject(data);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(content)
            };
        }

        private HttpResponseMessage HandleCreateRawAssets(string content)
        {
            var toCreate = System.Text.Json.JsonSerializer.Deserialize<RawListWrapper<JsonElement>>(content);
            foreach (var item in toCreate.items)
            {
                AssetsRaw[item.key] = item.columns;
            }

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{}")
            };
        }
        private HttpResponseMessage HandleCreateRawTimeseries(string content)
        {
            var toCreate = System.Text.Json.JsonSerializer.Deserialize<RawListWrapper<JsonElement>>(content);
            foreach (var item in toCreate.items)
            {
                TimeseriesRaw[item.key] = item.columns;
            }
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{}")
            };
        }
        private HttpResponseMessage HandleCreateRawRelationships(string content)
        {
            var toCreate = JsonConvert.DeserializeObject<RawListWrapper<RelationshipDummy>>(content);
            foreach (var item in toCreate.items)
            {
                RelationshipsRaw[item.key] = item.columns;
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
                        missing = missing.Select(id => new CdfIdentity { externalId = id }),
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
                            log.LogInformation("Add metadata: {Field}", field.Key);
                            old.metadata[field.Key] = field.Value;
                        }
                    }
                }
                ret.Add(old);
            }
            var result = new ReadWrapper<AssetDummy>
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
                        missing = missing.Select(id => new CdfIdentity { externalId = id }),
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
                            log.LogInformation("Add metadata: {Field}", field.Key);
                            old.metadata[field.Key] = field.Value;
                        }
                    }
                }
                ret.Add(old);
            }
            var result = new ReadWrapper<TimeseriesDummy>
            {
                items = ret,
            };
            return new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(JsonConvert.SerializeObject(result))
            };
        }

        private HttpResponseMessage HandleDeleteRelationships(string content)
        {
            var data = JsonConvert.DeserializeObject<ItemsWithoutCursor<CogniteExternalId>>(content);
            foreach (var id in data.Items)
            {
                Relationships.Remove(id.ExternalId);
            }
            return new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent("{}")
            };
        }

        public AssetDummy MockAsset(string externalId)
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

        public TimeseriesDummy MockTimeseries(string externalId)
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

        private HttpResponseMessage HandleCreateRelationships(string content)
        {
            var newRelationships = JsonConvert.DeserializeObject<RelationshipsReadWrapper>(content);

            var duplicated = new List<CdfIdentity>();
            var created = new List<(string Id, RelationshipDummy Rel)>();
            foreach (var rel in newRelationships.items)
            {
                if (Relationships.ContainsKey(rel.externalId) || created.Any(ct => ct.Id == rel.externalId))
                {
                    duplicated.Add(new CdfIdentity { externalId = rel.externalId });
                    continue;
                }
                if (duplicated.Count != 0) continue;
                rel.createdTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
                rel.lastUpdatedTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
                created.Add((rel.externalId, rel));
            }
            if (duplicated.Count != 0)
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
            foreach ((string id, var relDummy) in created)
            {
                Relationships.Add(id, relDummy);
            }
            string result = JsonConvert.SerializeObject(newRelationships);
            return new HttpResponseMessage(HttpStatusCode.Created)
            {
                Content = new StringContent(result)
            };
        }

        private HttpResponseMessage HandleRetrieveDataSets(string content)
        {
            var ids = JsonConvert.DeserializeObject<IdentityListWrapper>(content);
            var items = new List<DataSet>();

            foreach (var id in ids.items)
            {
                items.Add(DataSets[id.externalId]);
            }
            var data = JsonConvert.SerializeObject(new ItemWrapper<DataSet>
            {
                Items = items
            }, new JsonSerializerSettings
            {
                ContractResolver = new DefaultContractResolver { NamingStrategy = new CamelCaseNamingStrategy() }
            });
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(data)
            };
        }

        private HttpResponseMessage HandleCreateSpaces(string content)
        {
            var data = System.Text.Json.JsonSerializer.Deserialize<ItemsWithoutCursor<Space>>(content,
                Oryx.Cognite.Common.jsonOptions);
            foreach (var dt in data.Items)
            {
                Spaces[dt.Space] = dt;
            }
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(content)
            };
        }

        private HttpResponseMessage HandleCreateViews(string content)
        {
            var data = System.Text.Json.JsonSerializer.Deserialize<ItemsWithoutCursor<JsonObject>>(content,
                Oryx.Cognite.Common.jsonOptions);
            foreach (var dt in data.Items)
            {
                Views[(string)dt["externalId"].AsValue()] = dt;
            }
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(content)
            };
        }

        private HttpResponseMessage HandleCreateContainers(string content)
        {
            var data = System.Text.Json.JsonSerializer.Deserialize<ItemsWithoutCursor<JsonObject>>(content,
                Oryx.Cognite.Common.jsonOptions);
            foreach (var dt in data.Items)
            {
                Containers[(string)dt["externalId"].AsValue()] = dt;
            }
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(content)
            };
        }

        private HttpResponseMessage HandleCreateDataModels(string content)
        {
            var data = System.Text.Json.JsonSerializer.Deserialize<ItemsWithoutCursor<JsonObject>>(content,
                Oryx.Cognite.Common.jsonOptions);
            foreach (var dt in data.Items)
            {
                DataModels[(string)dt["externalId"].AsValue()] = dt;
            }
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(content)
            };
        }

        private HttpResponseMessage HandleCreateInstances(string content)
        {
            var data = System.Text.Json.JsonSerializer.Deserialize<ItemsWithoutCursor<JsonObject>>(content,
                Oryx.Cognite.Common.jsonOptions);
            foreach (var dt in data.Items)
            {
                Instances[(string)dt["externalId"].AsValue()] = dt;
            }
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(content)
            };
        }

        private HttpResponseMessage HandleFilterInstances(string content)
        {
            var results = new List<BaseInstance<JsonObject>>();

            var req = System.Text.Json.JsonSerializer.Deserialize<InstancesFilter>(content, Oryx.Cognite.Common.jsonOptions);
            if (req.InstanceType == InstanceType.edge && req.Filter is OrFilter orF && orF.Or.First() is InFilter)
            {
                var filters = orF.Or.OfType<InFilter>().ToList();
                Assert.Equal(2, filters.Count);

                var startNodes = filters
                    .First(f => f.Property.ElementAt(1) == "startNode")
                    .Values
                    .Select(v => (v as RawPropertyValue<JsonElement>).Value
                        .Deserialize<DirectRelationIdentifier>(Oryx.Cognite.Common.jsonOptions).ExternalId)
                    .ToHashSet();
                var endNodes = filters
                    .First(f => f.Property.ElementAt(1) == "endNode")
                    .Values
                    .Select(v => (v as RawPropertyValue<JsonElement>).Value
                        .Deserialize<DirectRelationIdentifier>(Oryx.Cognite.Common.jsonOptions).ExternalId)
                    .ToHashSet();

                foreach (var inst in Instances.Values)
                {
                    if (inst["instanceType"].GetValue<string>() == "edge"
                        && (startNodes.Contains(inst["startNode"]["externalId"].GetValue<string>())
                            || endNodes.Contains(inst["endNode"]["externalId"].GetValue<string>())))
                    {
                        results.Add(inst.Deserialize<Edge<JsonObject>>(Oryx.Cognite.Common.jsonOptions));
                    }
                }
            }

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(System.Text.Json.JsonSerializer.Serialize(new ItemsWithCursor<BaseInstance<JsonObject>>
                {
                    Items = results,
                    NextCursor = null
                }, Oryx.Cognite.Common.jsonOptions))
            };
        }

        private HttpResponseMessage HandleDeleteInstances(string content)
        {
            var req = System.Text.Json.JsonSerializer.Deserialize<ItemsWithoutCursor<InstanceIdentifier>>(content, Oryx.Cognite.Common.jsonOptions);
            foreach (var id in req.Items)
            {
                Instances.Remove(id.ExternalId);
            }

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{}")
            };
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
    public class AssetDummyJson : AssetDummy
    {
        public new JsonElement metadata { get; set; }
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
    public class CdfIdentity
    {
        public string externalId { get; set; }
    }
    public class IdentityListWrapper
    {
        public IEnumerable<CdfIdentity> items { get; set; }
        public bool ignoreUnknownIds { get; set; }
    }
    public class ErrorContent
    {
        public int code { get; set; }
        public string message { get; set; }
        public IEnumerable<CdfIdentity> missing { get; set; }
        public IEnumerable<CdfIdentity> duplicated { get; set; } = new List<CdfIdentity>();
    }
    public class ErrorWrapper
    {
        public ErrorContent error { get; set; }
    }
    public class ReadWrapper<T>
    {
        public IEnumerable<T> items { get; set; }
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
        public new JsonElement metadata { get; set; }
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
    public class RelationshipDummy
    {
        public string externalId { get; set; }
        public string sourceExternalId { get; set; }
        public string sourceType { get; set; }
        public string targetExternalId { get; set; }
        public string targetType { get; set; }
        public long createdTime { get; set; }
        public long lastUpdatedTime { get; set; }
        public bool deleted { get; set; }
    }
    public class RelationshipsReadWrapper
    {
        public IEnumerable<RelationshipDummy> items { get; set; }
    }
    public class FunctionCallWrapper<T>
    {
        public T Data { get; set; }
    }
}
#pragma warning restore IDE1006 // Naming Styles
