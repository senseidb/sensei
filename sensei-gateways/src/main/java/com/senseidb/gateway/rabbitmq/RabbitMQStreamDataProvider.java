package com.senseidb.gateway.rabbitmq;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

import com.senseidb.indexing.DataSourceFilter;

/**
 * @author shixin
 * @email shixin@xiaomi.com
 */
public class RabbitMQStreamDataProvider extends StreamDataProvider<JSONObject> {
  private static final Logger _logger = LoggerFactory.getLogger(RabbitMQStreamDataProvider.class);

  private final ScheduledExecutorService _scheduledExecutor;
  private final BlockingQueue<JSONObject> _filteredData;
  private RabbitMQConsumerManager[] _rabbitMQConsumerManagers;

  private RabbitMQConfig[] _rabbitMQConfigs;
  private DataSourceFilter<byte[]> _dataFilter;

  private volatile boolean _isStarted = false;

  public RabbitMQStreamDataProvider(Comparator<String> versionComparator,
      RabbitMQConfig[] rabbitMQConfigs, DataSourceFilter<byte[]> dataFilter, String Oldsincekey) {
    super(versionComparator);
    _rabbitMQConfigs = rabbitMQConfigs;
    _dataFilter = dataFilter;

    _scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    _filteredData = new ArrayBlockingQueue<JSONObject>(100000, true);
  }

  @Override
  public void start() {
    _logger.info("Trying to start RabbitMQStreamDataProvider...");
    if (_isStarted) return;

    _rabbitMQConsumerManagers = new RabbitMQConsumerManager[_rabbitMQConfigs.length];
    for (int i = 0; i < _rabbitMQConfigs.length; i++) {
      _rabbitMQConsumerManagers[i] = new RabbitMQConsumerManager(_rabbitMQConfigs[i], this);
    }

    // Check every RabbitMQ manager.
    _scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        for (RabbitMQConsumerManager manager : _rabbitMQConsumerManagers) {
          if (manager.getConsumerFailed()) {
            _logger.info("RabbitMQConsumerManager has failed worker, ready to restart...");
            _logger.info("RabbitMQConsumerManager config : {}", manager.toString());
            try {
              manager.stop();
              manager.start();
            } catch (InterruptedException e) { // Don't need to restart consumer manager now.
              Thread.currentThread().interrupt();
              return;
            } catch (IOException e) {
              _logger.error("Failed to start RabbitMQConsumerManager.", e);
            } catch (Throwable t) {
              _logger.error("Meet unknown throwable to start RabbitMQConsumerManager.", t);
            }
          }
        }
      }
    }, 0, 1000 * 10, TimeUnit.MILLISECONDS); // 10 seconds

    // check BlockingQueue
    _scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        _logger.info("Blocking Queue's size is {}", getQueueSize());
      }
    }, 0, 1000 * 60, TimeUnit.MILLISECONDS);

    super.start();
    _isStarted = true;
  }

  @Override
  public void stop() {
    _logger.info("Trying to stop RabbitMQStreamDataProvider...");
    if (!_isStarted) return;

    boolean isInterrupted = false;

    _scheduledExecutor.shutdown();
    try {
      _scheduledExecutor.awaitTermination(1000, TimeUnit.MICROSECONDS);
    } catch (InterruptedException e) {
      _logger.error(e.getMessage(), e);
      isInterrupted = true;
    }

    for (RabbitMQConsumerManager manager : _rabbitMQConsumerManagers) {
      try {
        manager.stop();
      } catch (InterruptedException e) {
        _logger.error(e.getMessage(), e);
        isInterrupted = true;
      }
    }

    if (isInterrupted) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
    }

    super.stop();
    _isStarted = false;
  }

  @Override
  public DataEvent<JSONObject> next() {
    if (!_isStarted) return null;

    JSONObject filteredData = getFilteredDataFromQueue();

    if (null == filteredData) return null;

    long version = System.currentTimeMillis();
    DataEvent<JSONObject> dataEvent = new DataEvent<JSONObject>(filteredData,
        String.valueOf(version));
    _logger.info("Successfully consume DataEvent : {}", dataEvent.getData().toString());

    return dataEvent;
  }

  JSONObject getFilteredDataFromQueue() {
    try {
      return _filteredData.take();
    } catch (InterruptedException e) {
      _logger.error("Meet InterruptedException when trying to get filtered data from queue.", e);
      return null;
    }
  }

  void putFilteredIntoQueue(JSONObject filteredData) {
    try {
      _filteredData.put(filteredData);
    } catch (InterruptedException e) {
      _logger.error("Meet InterruptedException when trying to put filtered data into queue.", e);
    }
  }

  int getQueueSize() {
    if (null != _filteredData) {
      return _filteredData.size();
    }
    return 0;
  }

  DataSourceFilter<byte[]> getDataSourceFilter() {
    return this._dataFilter;
  }

  @Override
  public void reset() {
  }

  @Override
  public void setStartingOffset(String arg0) {
  }
}
