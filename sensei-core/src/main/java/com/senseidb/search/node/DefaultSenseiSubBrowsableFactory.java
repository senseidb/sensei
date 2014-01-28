package com.senseidb.search.node;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.api.Browsable;
import com.senseidb.search.req.SenseiRequest;
import java.util.List;

public class DefaultSenseiSubBrowsableFactory implements SenseiSubBrowsableFactory {
  @Override
  public Browsable[] createBrowsables(List<BoboSegmentReader> readerList, SenseiCore core, SenseiRequest request) throws Exception {
    return BoboBrowser.createBrowsables(readerList);
  }
}
