package com.senseidb.search.node;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.api.Browsable;
import com.senseidb.search.req.SenseiRequest;
import java.util.List;

public interface SenseiSubBrowsableFactory {
  Browsable[] createBrowsables(List<BoboSegmentReader> readerList, SenseiCore core, SenseiRequest request) throws Exception;
}
