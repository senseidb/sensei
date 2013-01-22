
package com.senseidb.gateway.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author shixin
 * @email shixin@xiaomi.com
 */
class RabbitMQConsumerManager {
    private static final Logger _logger = LoggerFactory.getLogger(RabbitMQConsumerManager.class);

    private volatile boolean _isStarted = false;
    private final RabbitMQConfig _rabbitMQConfig;
    private final ConnectionFactory _connectionFactory;
    private Connection _connection;
    private Channel _channel;
    private QueueingConsumer _consumer;

    RabbitMQConsumerManager(RabbitMQConfig config) {
        _rabbitMQConfig = config;

        // Create an RabbitMQ connection factory
        _connectionFactory = new ConnectionFactory();
        _connectionFactory.setHost(config.getConnectionConfig().getHost());
        if (config.getConnectionConfig().getPort() > 0) {
            _connectionFactory.setPort(config.getConnectionConfig().getPort());
        }
        _connectionFactory.setRequestedHeartbeat(config.getConnectionConfig().getHeartbeat());
        _connectionFactory.setConnectionTimeout(config.getConnectionConfig().getTimeout());
    }

    synchronized boolean isStarted() {
        return _isStarted;
    }

    synchronized void start() throws IOException {
        if (_isStarted)
            return;

        // Create an RabbitMQ connection
        _connection = _connectionFactory.newConnection();

        // Get an RabbitMQ channel, and do some configurations
        _channel = _connection.createChannel();
        _channel.queueDeclare(_rabbitMQConfig.getQueueConfig().getName(), _rabbitMQConfig.getQueueConfig().isDurable(), _rabbitMQConfig
                .getQueueConfig().isExclusive(), _rabbitMQConfig.getQueueConfig().isAutodelete(), null);
        if (_rabbitMQConfig.getExchangeConfig() != null) {
            _channel.exchangeDeclare(_rabbitMQConfig.getExchangeConfig().getName(), _rabbitMQConfig.getExchangeConfig().getType(),
                _rabbitMQConfig.getExchangeConfig().isDurable());
            _channel.queueBind(_rabbitMQConfig.getQueueConfig().getName(), _rabbitMQConfig.getExchangeConfig().getName(), _rabbitMQConfig
                    .getQueueConfig().getRoutingKey());
        }
        _channel.basicQos(1);

        // Get RabbitMQ consumer
        boolean autoAck = false;
        _consumer = new QueueingConsumer(_channel);
        _channel.basicConsume(_rabbitMQConfig.getQueueConfig().getName(), autoAck, _consumer);

        _logger.info("Successfully open connection to RabbitMQ Server : {}", _rabbitMQConfig.getConnectionConfig());
        _isStarted = true;
    }

    synchronized void stop() {
        if (!_isStarted)
            return;

        if (_channel != null && _channel.isOpen()) {
            try {
                _channel.close();
                _logger.error("Channel is closed successfully");
            } catch (IOException e) {
                _logger.error("Failed to close rabbitMQ channel!", e);
            }
        }

        if (_connection != null && _connection.isOpen()) {
            try {
                _connection.close();
                _logger.info("Connection is closed successfully");
            } catch (IOException e) {
                _logger.error("Failed to close rabbitMQ connection!", e);
            }
        }

        _isStarted = false;
    }

    synchronized QueueingConsumer getConsumer() {
        return _consumer;
    }

    synchronized void basicAck(Delivery delivery) throws IOException {
        _channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    }
}
