
package com.senseidb.gateway.rabbitmq;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.gateway.rabbitmq.RabbitMQConfig.ConnectionConfig;
import com.senseidb.gateway.rabbitmq.RabbitMQConfig.ExchangeConfig;
import com.senseidb.gateway.rabbitmq.RabbitMQConfig.QueueConfig;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.zoie.impl.indexing.StreamDataProvider;

import java.util.Comparator;
import java.util.Set;

/**
 * @author shixin
 * @email shixin@xiaomi.com
 */
public class RabbitMQDataProviderBuilder extends SenseiGateway<byte[]> {
    private static final Logger _logger = LoggerFactory.getLogger(RabbitMQDataProviderBuilder.class);
    
    // RabbitMQ servers configuration
    private RabbitMQConfig[] _mqConfigs;

    // sensei.properties rabbitmq parameters
    private static final String RABBIT_MQ_SERVERS = "rabbitmq.servers.names";

    private static final String RABBIT_MQ_CONNECTION_HOST = ".host";
    private static final String RABBIT_MQ_CONNECTION_PORT = ".port";

    private static final String RABBIT_MQ_EXCHANGE_NAME = ".exchange.name";
    private static final String RABBIT_MQ_EXCHANGE_TYPE = ".exchange.type";
    private static final String RABBIT_MQ_EXCHANGE_DURABLE = ".exchange.durable";

    private static final String RABBIT_MQ_QUEUE_NAME = ".queue.name";
    private static final String RABBIT_MQ_QUEUE_ROUTING_KEY = ".queue.routing.key";
    private static final String RABBIT_MQ_QUEUE_DURABLE = ".queue.durable";
    private static final String RABBIT_MQ_QUEUE_EXCLUSIVE = ".queue.exclusive";
    private static final String RABBIT_MQ_QUEUE_AUTO_DELETE = ".queue.auto.delete";
    private static final String RABBIT_MQ_CONSUMER_COUNT = ".consumer.count";
    
    private static final String MAX_PARTITION_ID = "maxpartition.id";
    
    
    /**
     * example:
     * # rabbitMQ gateway parameters
     * sensei.gateway.class=com.senseidb.gateway.rabbitmq.RabbitMQDataProviderBuilder
     * sensei.gateway.rabbitmq.servers.names=server1,server2
     * sensei.gateway.server1.host=192.168.1.9
     * sensei.gateway.server1.exchange.name=userInfoUpdate_in_exchange
     * sensei.gateway.server1.exchange.type=direct
     * sensei.gateway.server1.exchange.durable=false
     * sensei.gateway.server1.queue.name=sensei_search_users_1
     * sensei.gateway.server1.queue.routing.key=userInfoUpdate
     * sensei.gateway.server1.queue.durable=true
     * sensei.gateway.server1.queue.exclusive=false
     * sensei.gateway.server1.queue.auto.delete=false
     * sensei.gateway.server1.consumer.count=5
     * sensei.gateway.server2.host=192.168.1.11
     * sensei.gateway.server2.exchange.name=userInfoUpdate_in_exchange
     * sensei.gateway.server2.exchange.type=direct
     * sensei.gateway.server2.exchange.durable=false
     * sensei.gateway.server2.queue.name=sensei_search_users_1
     * sensei.gateway.server2.queue.routing.key=userInfoUpdate
     * sensei.gateway.server2.queue.durable=true
     * sensei.gateway.server2.queue.exclusive=false
     * sensei.gateway.server2.queue.auto.delete=false
     * sensei.gateway.server2.consumer.count=5
     */
    private void initRabbitMQConfigs() {
        _logger.info("Start construct RabbitMQ servers configurations.");

        String rabbitMQServersNames = config.get(RABBIT_MQ_SERVERS);
        Validate.notEmpty(rabbitMQServersNames, "rabbitmq.servers.names can't be empty!");

        String[] rabbitmqServers = rabbitMQServersNames.split(",");
        Validate.notEmpty(rabbitmqServers, "RabbitMQ servers can't be empty");

        _mqConfigs = new RabbitMQConfig[rabbitmqServers.length];
        for (int i = 0; i < _mqConfigs.length; i++) {
            String rabbitmqServer = rabbitmqServers[i].trim();
            if (StringUtils.isEmpty(rabbitmqServer))
                continue;

            // 1:RabbitMQ connection config
            String host = config.get(rabbitmqServer + RABBIT_MQ_CONNECTION_HOST);
            Validate.notEmpty(host, "RabbitMQ host can't be empty!");
            String port = config.get(rabbitmqServer + RABBIT_MQ_CONNECTION_PORT);
            int portInt = -1;
            if (StringUtils.isNotEmpty(port))
                try {
                    portInt = Integer.parseInt(port);
                } catch (NumberFormatException e) {
                    portInt = -1;
                }
            ConnectionConfig connectionConfig = new ConnectionConfig(host, portInt);

            // 2:RabbitMQ exchange config
            ExchangeConfig exchangeConfig = null;
            String exchangeName = config.get(rabbitmqServer + RABBIT_MQ_EXCHANGE_NAME);
            String exchangeType = config.get(rabbitmqServer + RABBIT_MQ_EXCHANGE_TYPE);
            String exchangeDurable = config.get(rabbitmqServer + RABBIT_MQ_EXCHANGE_DURABLE);
            if (StringUtils.isNotEmpty(exchangeName)) {
                Validate.notEmpty(exchangeType, "Must give a exchage type!");
                exchangeConfig = new ExchangeConfig(exchangeName, exchangeType, Boolean.valueOf(exchangeDurable));
            }

            // 3:RabbitMQ queue config
            QueueConfig queueConfig = null;
            String queueName = config.get(rabbitmqServer + RABBIT_MQ_QUEUE_NAME);
            Validate.notEmpty(queueName, "Must give a queue name!");
            boolean queueDurable = Boolean.valueOf(config.get(rabbitmqServer + RABBIT_MQ_QUEUE_DURABLE));
            boolean queueExclusive = Boolean.valueOf(config.get(rabbitmqServer + RABBIT_MQ_QUEUE_EXCLUSIVE));
            boolean queueAutoDelete = Boolean.valueOf(config.get(rabbitmqServer + RABBIT_MQ_QUEUE_AUTO_DELETE));
            String queueRoutingKey = config.get(rabbitmqServer + RABBIT_MQ_QUEUE_ROUTING_KEY);
            if (StringUtils.isNotEmpty(queueRoutingKey))
                queueConfig = new QueueConfig(queueName, queueDurable, queueExclusive, queueAutoDelete, queueRoutingKey);
            else
                queueConfig = new QueueConfig(queueName, queueDurable, queueExclusive, queueAutoDelete);

            _mqConfigs[i] = new RabbitMQConfig(connectionConfig, exchangeConfig, queueConfig);

            try {
                _mqConfigs[i].setConsumeWorkerCount(Integer.parseInt(config.get(rabbitmqServer + RABBIT_MQ_CONSUMER_COUNT)));
            } catch (NumberFormatException e) {
                // do nothing
            }

            _logger.info("Successfully construct RabbitMQ server : {}", _mqConfigs[i]);
        }
        _logger.info("Successfully construct RabbitMQ servers configurations.");
    }

    @Override
    public Comparator<String> getVersionComparator() {
        return DEFAULT_VERSION_COMPARATOR;
    }

    @Override
    public StreamDataProvider<JSONObject> buildDataProvider(DataSourceFilter<byte[]> dataFilter, String Oldsincekey,
        ShardingStrategy shardingStrategy, Set<Integer> partitions) throws Exception {
        initRabbitMQConfigs();
        int maxPartitionId = Integer.parseInt(config.get(MAX_PARTITION_ID));
        return new RabbitMQStreamDataProvider(DEFAULT_VERSION_COMPARATOR, _mqConfigs, maxPartitionId, dataFilter, Oldsincekey,
                shardingStrategy, partitions);
    }

}
