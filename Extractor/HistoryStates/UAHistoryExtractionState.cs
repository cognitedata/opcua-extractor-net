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

using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Opc.Ua;
using System;

namespace Cognite.OpcUa.HistoryStates
{
    public class UAHistoryExtractionState : HistoryExtractionState
    {
        public NodeId SourceId { get; }
        public bool Initialized { get; private set; }
        public UAHistoryExtractionState(UAExtractor extractor, NodeId id, bool frontfill, bool backfill)
            : base(extractor?.GetUniqueId(id), frontfill, backfill)
        {
            SourceId = id;
        }
        public UAHistoryExtractionState(UAClient client, NodeId id, bool frontfill, bool backfill)
            : base(client?.GetUniqueId(id), frontfill, backfill)
        {
            SourceId = id;
        }

        public void InitToEmpty()
        {
            lock (_mutex)
            {
                if (!FrontfillEnabled || BackfillEnabled)
                {
                    SourceExtractedRange = DestinationExtractedRange = new TimeRange(DateTime.UtcNow, DateTime.UtcNow);
                }
                else
                {
                    SourceExtractedRange = DestinationExtractedRange = new TimeRange(CogniteTime.DateTimeEpoch, CogniteTime.DateTimeEpoch);
                }
            }
        }
        public override void FinalizeRangeInit()
        {
            Initialized = true;
            base.FinalizeRangeInit();
        }
    }
}
