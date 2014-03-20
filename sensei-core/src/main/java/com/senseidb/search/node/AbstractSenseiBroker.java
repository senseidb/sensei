package com.senseidb.search.node;

import org.apache.log4j.Logger;

import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.senseidb.svc.api.SenseiException;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 * @param <REQUEST>
 * @param <RESULT>
 */
public abstract class AbstractSenseiBroker<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult>
    implements Broker<REQUEST, RESULT> {
  private final static Logger logger = Logger.getLogger(AbstractSenseiBroker.class);

  /**
   * @return an empty result instance. Used when the request cannot be properly
   *         processed or when the true result is empty.
   */
  public abstract RESULT getEmptyResultInstance();

  /**
   * The method that provides the search service.
   *
   * @param req
   * @return
   * @throws SenseiException
   */
  @Override
  public abstract RESULT browse(final REQUEST req) throws SenseiException;

  public void shutdown() {
    logger.info("shutting down broker...");
  }
}
