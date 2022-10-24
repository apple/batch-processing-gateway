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

import java.io.File;
import java.sql.Connection;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DBConnectionTest {

  private static final String H2_DRIVER_CLASS = "org.h2.Driver";

  @Test
  public void test() throws Exception {
    Class.forName(H2_DRIVER_CLASS);

    File file = File.createTempFile("h2_test_db", ".h2db");
    file.deleteOnExit();

    String connectionString =
        String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=PostgreSQL", file.getAbsolutePath());

    DBConnection dbConnection = new DBConnection(connectionString);
    try (Connection connection = dbConnection.getConnection()) {
      Assert.assertNotNull(connection);
    }
    dbConnection.close();
  }
}
