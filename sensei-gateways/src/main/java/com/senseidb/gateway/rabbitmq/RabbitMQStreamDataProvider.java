
package com.senseidb.gateway.rabbitmq;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

import java.io.IOException;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author shixin
 * @email shixin@xiaomi.com
 */
public class RabbitMQStreamDataProvider extends StreamDataProvider<JSONObject> {
    private static final Logger _logger = LoggerFactory.getLogger(RabbitMQStreamDataProvider.class);
    private static final ScheduledExecutorService _SCHEDULED_EXECUTOR = Executors.newSingleThreadScheduledExecutor();
    private static int RABBIT_MQ_SERVER_POINTOR = 0;	// Point at the current RabbitMQ server
    private static int RABBIT_MQ_SERVER_COUNT = 0;	// The total number of the RabbitMQ servers

    private boolean _isStarted = false;
    private static final int _CHECK_INTERVAL = 1000 * 10;
    private static final int _RABBITMQ_CONSUMER_DELIVERY_TIMEOUT = 1000;
    private RabbitMQConfig[] _rabbitMQConfigs;
    private int _maxPartitionId;
    private RabbitMQConsumerManager[] _rabbitMQConsumerManagers;
    private DataSourceFilter<byte[]> _dataFilter;
    private ShardingStrategy _shardingStrategy;
    private Set<Integer> _partitions;

    public RabbitMQStreamDataProvider(Comparator<String> versionComparator, RabbitMQConfig[] rabbitMQConfigs, int maxPartitionId,
                                      DataSourceFilter<byte[]> dataFilter, String Oldsincekey, ShardingStrategy shardingStrategy,
                                      Set<Integer> partitions) {
        super(versionComparator);
        _rabbitMQConfigs = rabbitMQConfigs;
        _maxPartitionId = maxPartitionId;
        _dataFilter = dataFilter;
        _shardingStrategy = shardingStrategy;
        _partitions = partitions;
        RABBIT_MQ_SERVER_COUNT = rabbitMQConfigs.length;
    }

    @Override
    public void start() {
        if (_isStarted)
            return;

        _rabbitMQConsumerManagers = new RabbitMQConsumerManager[_rabbitMQConfigs.length];
        for (int i = 0; i < _rabbitMQConfigs.length; i++) {
            _rabbitMQConsumerManagers[i] = new RabbitMQConsumerManager(_rabbitMQConfigs[i]);
        }
        
        // Check the RabbitMQ servers' connections.
        _SCHEDULED_EXECUTOR.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                for (RabbitMQConsumerManager manager : _rabbitMQConsumerManagers) {
                    if (!manager.isStarted()) {
                        try {
                            manager.stop();
                            manager.start();
                        } catch (IOException e) {
                            _logger.error("Failed to start RabbitMQConsumerManager.", e);
                        } catch (Throwable t) {
                            _logger.error("Meet unknown throwable to start RabbitMQConsumerManager.", t);
                        }
                    }
                }
            }
        }, 0, _CHECK_INTERVAL, TimeUnit.MILLISECONDS);

        super.start();
        _isStarted = true;
    }

    @Override
    public void stop() {
        if (!_isStarted)
            return;

        boolean isInterrupted = false;

        _SCHEDULED_EXECUTOR.shutdown();
        try {
            _SCHEDULED_EXECUTOR.awaitTermination(1000, TimeUnit.MICROSECONDS);
        } catch (InterruptedException e) {
            isInterrupted = true;
            _logger.error(e.getMessage(), e);
        }

        for (RabbitMQConsumerManager manager : _rabbitMQConsumerManagers)
            manager.stop();

        super.stop();
        _isStarted = false;

        if (isInterrupted) {
            // Restore the interrupted status
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public DataEvent<JSONObject> next() {
        if (!_isStarted)
            return null;

        RabbitMQConsumerManager manager = _rabbitMQConsumerManagers[RABBIT_MQ_SERVER_POINTOR];
        if (manager.isStarted() && null != manager.getConsumer()) {
            QueueingConsumer consumer = manager.getConsumer();
            try {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery(_RABBITMQ_CONSUMER_DELIVERY_TIMEOUT);
                if (null == delivery)
                    return null;
                
                JSONObject jsonObject = _dataFilter.filter(delivery.getBody());
                manager.basicAck(delivery);
                incrementRabbitMQServerPointor();
                
                if(null == jsonObject)
                    return null;
                
                if(!_partitions.contains(_shardingStrategy.caculateShard(_maxPartitionId + 1, new JSONObject().put("data", jsonObject))))
                    return null;
                
                long version = System.currentTimeMillis();
                DataEvent<JSONObject> dataEvent = new DataEvent<JSONObject>(jsonObject, String.valueOf(version));
                _logger.info("Successfully generate DataEvent : {}", dataEvent.getData().toString());
                return dataEvent;
            } catch (ShutdownSignalException e) {
                _logger.error("Found exception when tring to get delivery.", e);
            } catch (ConsumerCancelledException e) {
                _logger.error("Found exception when tring to get delivery.", e);
            } catch (InterruptedException e) {
                _logger.error("Found exception when tring to get delivery.", e);
            } catch (Exception e) {
                _logger.error("Found exception when do data filter.", e);
            }
        }

        incrementRabbitMQServerPointor();
        return null;
    }

    @Override
    public void reset() {
    }

    @Override
    public void setStartingOffset(String arg0) {
    }

    private synchronized void incrementRabbitMQServerPointor() {
        RABBIT_MQ_SERVER_POINTOR++;
        if (RABBIT_MQ_SERVER_COUNT == RABBIT_MQ_SERVER_POINTOR)
            RABBIT_MQ_SERVER_POINTOR = 0;
    }
}
