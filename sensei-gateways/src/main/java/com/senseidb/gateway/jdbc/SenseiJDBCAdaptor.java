package com.senseidb.gateway.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

public abstract class SenseiJDBCAdaptor {

  protected Set<Integer> partitions = null;

  abstract public PreparedStatement buildStatment(Connection conn, String fromVersion)
      throws SQLException;

  abstract public String extractVersion(ResultSet resultSet) throws SQLException;

  public void setPartitions(Set<Integer> partitions) {
    this.partitions = partitions;
  }
}
