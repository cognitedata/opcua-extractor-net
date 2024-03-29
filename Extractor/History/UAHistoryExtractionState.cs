﻿/* Cognite Extractor for OPC-UA
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

using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Opc.Ua;
using System;

namespace Cognite.OpcUa.History
{
    /// <summary>
    /// History extraction state.
    /// 
    /// History in the utils works using a two-step process, which we use extensively:
    /// 
    ///  - History is read from the source, and SourceExtractedRange is updated.
    ///  - Data is written to destinations, and DestinationExtractedRange is updated.
    ///  
    /// This lets us consider SourceExtractedRange our internal history range, and DestinationExtractedRange
    /// a subset of this, which is the data actually committed to CDF.
    /// </summary>
    public class UAHistoryExtractionState : HistoryExtractionState
    {
        public NodeId SourceId { get; }
        public bool Initialized { get; private set; }
        public UAHistoryExtractionState(
            IUAClientAccess client,
            NodeId id,
            bool frontfill, bool backfill)
            : base(client.GetUniqueId(id)!, frontfill, backfill)
        {
            SourceId = id;
        }

        public void InitToEmpty()
        {
            lock (Mutex)
            {
                if (!FrontfillEnabled || BackfillEnabled)
                {
                    var now = DateTime.UtcNow;
                    SourceExtractedRange = DestinationExtractedRange = new TimeRange(now, now);
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
