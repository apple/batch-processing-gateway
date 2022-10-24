/*
 *
 * This source file is part of the Batch Processing Gateway open source project
 *
 * Copyright 2022 Apple Inc. and the Batch Processing Gateway project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.spark.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBConnection {

  private static final Logger logger = LoggerFactory.getLogger(DBConnection.class);

  private final String connectionString;
  private final String user;
  private final String password;

  private volatile Connection connection;

  public DBConnection(String connectionString) {
    this(connectionString, null, null);
  }

  public DBConnection(String connectionString, String user, String password) {
    this.connectionString = connectionString;
    this.user = user;
    this.password = password;
  }

  public Connection getConnection() {
    if (connection != null) {
      try {
        if (!connection.isClosed()) {
          return connection;
        }
        connection.close();
      } catch (Throwable e) {
        logger.warn("Failed to check whether db connection is closed", e);
      }
    }

    try {
      if (StringUtils.isEmpty(user)) {
        connection = DriverManager.getConnection(connectionString);
      } else {
        connection = DriverManager.getConnection(connectionString, user, password);
      }
      return connection;
    } catch (Throwable e) {
      throw new RuntimeException(
          String.format("Failed to create db connection: %s", connectionString), e);
    }
  }

  public void executeSql(String sql) throws SQLException {
    logger.info("Executing SQL: {}", sql);
    getConnection().createStatement().execute(sql);
  }

  public void close() {
    if (connection != null) {
      try {
        connection.close();
        connection = null;
      } catch (Throwable e) {
        connection = null;
        logger.warn(String.format("Failed to close db connection: %s", connectionString), e);
      }
    }
  }
}
