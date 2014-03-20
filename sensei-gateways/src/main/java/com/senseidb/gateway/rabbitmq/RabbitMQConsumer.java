package com.senseidb.gateway.rabbitmq;

import java.io.IOException;

import org.apache.commons.lang.Validate;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.senseidb.indexing.DataSourceFilter;

public class RabbitMQConsumer implements Runnable {
  private static final Logger _logger = LoggerFactory.getLogger(RabbitMQConsumer.class);

  private String _name;
  private RabbitMQConsumerManager _rabbitMQConsumerManager;
  private Connection _connection;
  private RabbitMQConfig _rabbitMQConfig;
  private RabbitMQStreamDataProvider _rabbitMQStreamDataProvider;

  private volatile boolean _isRunning = false;

  RabbitMQConsumer(String name, RabbitMQConsumerManager _rabbitMQConsumerManager,
      Connection _connection, RabbitMQConfig _rabbitMQConfig,
      RabbitMQStreamDataProvider rabbitMQStreamDataProvider) {
    this._name = name;
    this._rabbitMQConsumerManager = _rabbitMQConsumerManager;
    this._connection = _connection;
    this._rabbitMQConfig = _rabbitMQConfig;
    this._rabbitMQStreamDataProvider = rabbitMQStreamDataProvider;
  }

  @Override
  public void run() {
    _logger.info("RabbitMQConsumer {} start to work!", _name);
    _isRunning = true;

    Channel channel = null;
    try {
      channel = _connection.createChannel();

      channel.queueDeclare(_rabbitMQConfig.getQueueConfig().getName(), _rabbitMQConfig
          .getQueueConfig().isDurable(), _rabbitMQConfig.getQueueConfig().isExclusive(),
        _rabbitMQConfig.getQueueConfig().isAutodelete(), null);
      if (_rabbitMQConfig.getExchangeConfig() != null) {
        channel.exchangeDeclare(_rabbitMQConfig.getExchangeConfig().getName(), _rabbitMQConfig
            .getExchangeConfig().getType(), _rabbitMQConfig.getExchangeConfig().isDurable());
        channel.queueBind(_rabbitMQConfig.getQueueConfig().getName(), _rabbitMQConfig
            .getExchangeConfig().getName(), _rabbitMQConfig.getQueueConfig().getRoutingKey());
      }
      channel.basicQos(1);

      boolean autoAck = false;
      QueueingConsumer consumer = new QueueingConsumer(channel);
      channel.basicConsume(_rabbitMQConfig.getQueueConfig().getName(), autoAck, consumer);

      while (_isRunning) {
        Delivery delivery = consumer.nextDelivery();

        if (null == delivery) continue;

        DataSourceFilter<byte[]> dataSourceFilter = this._rabbitMQStreamDataProvider
            .getDataSourceFilter();
        Validate.notNull(dataSourceFilter, "Fatal null pointor exception");

        JSONObject filteredData = dataSourceFilter.filter(delivery.getBody());
        if (null != filteredData) {
          this._rabbitMQStreamDataProvider.putFilteredIntoQueue(filteredData);
        }

        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      }
      _logger.info("RabbitMQConsumer {} job done!", _name);
    } catch (Exception e) {
      this._rabbitMQConsumerManager.setWorkerFailed(true, _name);
      if (e instanceof InterruptedException) {
        _logger.error("RabbitMQConsumer {} is stopped with interruptedException", _name, e);
      } else {
        _logger.error("RabbitMQConsumer {} is stopped with exception", _name, e);
      }
    } finally {
      if (null != channel && channel.isOpen()) {
        try {
          channel.close();
          _logger.info("RabbitMQConsumer {} successfully closed Channel");
        } catch (IOException e) {
          _logger.error("Failed to close RabbitMQ channel.");
        }
      }
    }
  }

  void stop() {
    _logger.info("RabbitMQConsumer {} is stoped by RabbitMQConsumerManager!", _name);
    _isRunning = false;
  }
}
