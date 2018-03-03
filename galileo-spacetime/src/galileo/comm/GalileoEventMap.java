/*
Copyright (c) 2014, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package galileo.comm;

import galileo.event.EventMap;

public class GalileoEventMap extends EventMap {
    public GalileoEventMap() {
        addMapping(10, DebugEvent.class);

        addMapping(100, StorageEvent.class);
        addMapping(101, StorageRequest.class);

        addMapping(200, QueryEvent.class);
        addMapping(201, QueryRequest.class);
        addMapping(202, QueryPreamble.class);
        addMapping(203, QueryResponse.class);
        
        addMapping(301, MetadataRequest.class);
        addMapping(302, MetadataResponse.class);
        addMapping(303, MetadataEvent.class);
        
        addMapping(401, BlockRequest.class);
        addMapping(402, BlockResponse.class);
        
        addMapping(501, FilesystemRequest.class);
        addMapping(502, FilesystemEvent.class);
        
        addMapping(601, DataIntegrationEvent.class);
        addMapping(602, DataIntegrationRequest.class);
        addMapping(603, DataIntegrationResponse.class);
        addMapping(604, DataIntegrationFinalResponse.class);
        
        addMapping(701, NeighborDataEvent.class);
        addMapping(702, NeighborDataResponse.class);
        
        addMapping(801, SurveyRequest.class);
        addMapping(802, SurveyResponse.class);
        addMapping(803, SurveyEvent.class);
        addMapping(804, SurveyEventResponse.class);
    }
}
