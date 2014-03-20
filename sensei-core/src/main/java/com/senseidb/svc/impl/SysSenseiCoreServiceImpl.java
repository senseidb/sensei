package com.senseidb.svc.impl;

import java.io.IOException;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import zu.finagle.serialize.JOSSSerializer;
import zu.finagle.serialize.ZuSerializer;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.api.MultiBoboBrowser;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiSystemInfo;

public class SysSenseiCoreServiceImpl extends
    AbstractSenseiCoreService<SenseiRequest, SenseiSystemInfo> {

  public static final ZuSerializer<SenseiRequest, SenseiSystemInfo> JAVA_SERIALIZER = new JOSSSerializer<SenseiRequest, SenseiSystemInfo>();
  public static final String MESSAGE_TYPE_NAME = "SenseiSysRequest";

  private static final Logger logger = Logger.getLogger(SysSenseiCoreServiceImpl.class);

  public SysSenseiCoreServiceImpl(SenseiCore core, Configuration conf) {
    super(core, conf);
  }

  @Override
  public SenseiSystemInfo handlePartitionedRequest(SenseiRequest request,
      List<BoboSegmentReader> readerList, SenseiQueryBuilderFactory queryBuilderFactory)
      throws Exception {
    SenseiSystemInfo res = new SenseiSystemInfo();

    MultiBoboBrowser browser = null;
    try {
      browser = new MultiBoboBrowser(readerList);
      res.setNumDocs(browser.numDocs());
      return res;
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw e;
    } finally {
      if (browser != null) {
        try {
          browser.close();
        } catch (IOException ioe) {
          logger.error(ioe.getMessage(), ioe);
        }
      }
    }
  }

  @Override
  public SenseiSystemInfo mergePartitionedResults(SenseiRequest r, List<SenseiSystemInfo> resultList) {
    long start = System.currentTimeMillis();
    SenseiSystemInfo result = _core.getSystemInfo();
    result.setNumDocs(0);
    long time = 0;
    for (SenseiSystemInfo res : resultList) {
      result.setNumDocs(result.getNumDocs() + res.getNumDocs());
      time = Math.max(time, res.getTime());
    }
    result.setTime(time + System.currentTimeMillis() - start);
    return result;
  }

  @Override
  public SenseiSystemInfo getEmptyResultInstance(Throwable error) {
    return new SenseiSystemInfo();
  }

  @Override
  public String getMessageTypeName() {
    return MESSAGE_TYPE_NAME;
  };

  @Override
  public ZuSerializer<SenseiRequest, SenseiSystemInfo> getSerializer() {
    return JAVA_SERIALIZER;
  }
}
