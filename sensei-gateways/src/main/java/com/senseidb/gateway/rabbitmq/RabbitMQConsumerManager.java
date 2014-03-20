package com.senseidb.gateway.rabbitmq;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

class RabbitMQConsumerManager {
  private static final Logger _logger = LoggerFactory.getLogger(RabbitMQConsumerManager.class);

  private final RabbitMQConfig _rabbitMQConfig;
  private RabbitMQStreamDataProvider _rabbitMQStreamDataProvider;

  private ConnectionFactory _connectionFactory;
  private Connection _connection;

  private volatile boolean _isStarted = false;
  private volatile boolean _isConsumerFailed = true;

  private RabbitMQConsumer[] _consumers;
  private Thread[] _consumerThreads;

  RabbitMQConsumerManager(RabbitMQConfig config,
      RabbitMQStreamDataProvider rabbitMQStreamDataProvider) {
    _rabbitMQConfig = config;
    _rabbitMQStreamDataProvider = rabbitMQStreamDataProvider;
    createConnectionFactory();
  }

  // Create an RabbitMQ connection factory
  private void createConnectionFactory() {
    _connectionFactory = new ConnectionFactory();
    _connectionFactory.setHost(_rabbitMQConfig.getConnectionConfig().getHost());
    _connectionFactory.setPort(_rabbitMQConfig.getConnectionConfig().getPort());
    _connectionFactory.setRequestedHeartbeat(_rabbitMQConfig.getConnectionConfig().getHeartbeat());
    _connectionFactory.setConnectionTimeout(_rabbitMQConfig.getConnectionConfig().getTimeout());
  }

  synchronized void start() throws IOException {
    if (_isStarted) return;

    // Create an RabbitMQ connection
    _connection = _connectionFactory.newConnection();
    _logger.info("Successfully open connection to RabbitMQ Server : {}",
      _rabbitMQConfig.getConnectionConfig());

    _consumers = new RabbitMQConsumer[_rabbitMQConfig.getConsumeWorkerCount()];
    _consumerThreads = new Thread[_rabbitMQConfig.getConsumeWorkerCount()];

    for (int i = 0; i < _rabbitMQConfig.getConsumeWorkerCount(); i++) {
      _consumers[i] = new RabbitMQConsumer(_rabbitMQConfig.getConnectionConfig().getHost() + ":"
          + i, this, _connection, _rabbitMQConfig, _rabbitMQStreamDataProvider);
      _consumerThreads[i] = new Thread(_consumers[i]);
      _consumerThreads[i].start();
    }

    _isConsumerFailed = false;
    _isStarted = true;
  }

  synchronized void stop() throws InterruptedException {
    if (!_isStarted) return;

    for (RabbitMQConsumer consumer : _consumers)
      consumer.stop();

    try {
      for (Thread consumerThread : _consumerThreads) {
        consumerThread.interrupt();
        consumerThread.join();
      }
    } finally {
      if (_connection != null && _connection.isOpen()) {
        try {
          _connection.close();
          _logger.info("Connection is closed successfully");
        } catch (IOException e) {
          _logger.error("Failed to close rabbitMQ connection!", e);
        }
      }

      _isConsumerFailed = true;
      _isStarted = false;
    }
  }

  void setWorkerFailed(boolean failed, String consumerName) {
    _logger.info("Set failed flag by consumer {}", consumerName);
    this._isConsumerFailed = failed;
  }

  boolean getConsumerFailed() {
    return this._isConsumerFailed;
  }
}
