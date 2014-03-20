package com.senseidb.gateway.rabbitmq;

/**
 * Represent a single rabbitmq server configuration.
 * 
 * @author ishikin
 * @email shixin42@gmail.com
 */
class RabbitMQConfig {
  private ConnectionConfig _connectionConfig;
  private ExchangeConfig _exchangeConfig;
  private QueueConfig _queueConfig;
  private int _consumeWorkerCount = 5;

  RabbitMQConfig(ConnectionConfig connectionConfig, ExchangeConfig exchangeConfig,
      QueueConfig queueConfig) {
    this._connectionConfig = connectionConfig;
    this._exchangeConfig = exchangeConfig;
    this._queueConfig = queueConfig;
  }

  ConnectionConfig getConnectionConfig() {
    return _connectionConfig;
  }

  ExchangeConfig getExchangeConfig() {
    return _exchangeConfig;
  }

  QueueConfig getQueueConfig() {
    return _queueConfig;
  }

  int getConsumeWorkerCount() {
    return _consumeWorkerCount;
  }

  void setConsumeWorkerCount(int consumeWorkerCount) {
    _consumeWorkerCount = consumeWorkerCount;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ConnectionConfig : " + _connectionConfig.toString()
        + ", ");
    if (null != _exchangeConfig) { // if no exchange configuration, rabbitmq
                                   // will use a anonymous
      sb.append("exchangeConfig : " + _exchangeConfig.toString() + ", ");
    }
    sb.append("queueConfig : " + _queueConfig.toString());
    return sb.toString();
  }

  /**
   * Represent a rabbitmq Connection configuration.
   */
  static class ConnectionConfig {
    private String _host = "localhost";
    private int _port = -1;
    private int _heartbeat = 5; // default value 5 seconds
    private int _timeout = 500; // default value 500 miliseconds for
                                // connection timeout

    ConnectionConfig(String host, int port) {
      this._host = host;
      this._port = port;
    }

    ConnectionConfig(String host, int port, int heartbeat, int timeout) {
      this._host = host;
      this._port = port;
      this._heartbeat = heartbeat;
      this._timeout = timeout;
    }

    String getHost() {
      return _host;
    }

    int getPort() {
      return _port;
    }

    int getHeartbeat() {
      return _heartbeat;
    }

    int getTimeout() {
      return _timeout;
    }

    @Override
    public String toString() {
      return "[host=" + this._host + ",port=" + this._port + ",heartbeat=" + this._heartbeat
          + ",timeout=" + this._timeout + "]";
    }
  }

  /**
   * Represent a rabbitmq Exchange configuration.
   */
  static class ExchangeConfig {
    private String _name;
    private String _type;
    private boolean _durable = false;

    ExchangeConfig(String name, String type) {
      this._name = name;
      this._type = type;
    }

    ExchangeConfig(String name, String type, boolean durable) {
      this._name = name;
      this._type = type;
      this._durable = durable;
    }

    String getName() {
      return _name;
    }

    boolean isDurable() {
      return _durable;
    }

    String getType() {
      return _type;
    }

    @Override
    public String toString() {
      return "[name=" + this._name + ",type=" + this._type + ",durable=" + this._durable + "]";
    }
  }

  /**
   * Represent a rabbitmq Queue configuration.
   */
  static class QueueConfig {
    private String _name;
    private boolean _durable = false;
    private boolean _exclusive = false;
    private boolean _autodelete = false;
    private String _routingkey = "";

    QueueConfig(String name, boolean durable, boolean exclusive, boolean autodelete,
        String routingkey) {
      this._name = name;
      this._durable = durable;
      this._exclusive = exclusive;
      this._autodelete = autodelete;
      this._routingkey = routingkey;
    }

    QueueConfig(String name, boolean durable, boolean exclusive, boolean autodelete) {
      this(name, durable, exclusive, autodelete, "");
    }

    String getName() {
      return _name;
    }

    boolean isDurable() {
      return _durable;
    }

    boolean isExclusive() {
      return _exclusive;
    }

    boolean isAutodelete() {
      return _autodelete;
    }

    String getRoutingKey() {
      return _routingkey;
    }

    @Override
    public String toString() {
      return "[name=" + this._name + ",durable=" + this._durable + ",exclusive=" + this._exclusive
          + ",autodelete=" + this._autodelete + ",routingkey=" + this._routingkey + "]";
    }
  }
}
