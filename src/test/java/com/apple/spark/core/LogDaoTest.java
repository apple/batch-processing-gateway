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

import static com.apple.spark.core.Constants.DEFAULT_DB_NAME;

import com.apple.spark.api.SubmitApplicationRequest;
import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import org.apache.commons.lang3.time.DateUtils;
import org.h2.util.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LogDaoTest {

  private static final String H2_DRIVER_CLASS = "org.h2.Driver";
  private final Double memoryMbSecondCost = 0.000000000372;
  private final Double vCoreSecondCost = 0.000003;

  @Test
  public void test() throws Exception {
    Class.forName(H2_DRIVER_CLASS);

    File file = File.createTempFile("h2_test_db", ".h2db");
    file.deleteOnExit();

    String connectionString =
        String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MySQL", file.getAbsolutePath());
    DBConnection dbConnection = new DBConnection(connectionString, null, null);

    LogDao logDao = new LogDao(connectionString, null, null, null);
    Assert.assertNotNull(logDao);

    SubmitApplicationRequest submission = new SubmitApplicationRequest();
    logDao.logApplicationSubmission("submission1", "user1", submission);
    logDao.logApplicationId("submission1", "app1");

    submission.setSparkVersion("2.4");
    logDao.logApplicationSubmission("submission1", "user1", submission);

    logDao.logApplicationId("submission2", "app2");
    logDao.logApplicationSubmission("submission2", "user1", submission);

    logDao.logApplicationSubmission("submission3", "user1", submission);
    logDao.logApplicationStatus("submission3", "");
    logDao.logApplicationStatus("submission3", "RUNNING");

    // you can change format of date
    String startTime1 = "2000-01-01T01:01:01Z";
    Timestamp startTimeStamp =
        new Timestamp(
            DateUtils.parseDate(startTime1, new String[] {"yyyy-MM-dd'T'HH:mm:ss'Z'"}).getTime());
    int driverCore = 4;
    int driverMemoryMb = 4096;
    int executorInstances = 10;
    int executorCore = 1;
    int executorMemoryMb = 1024;
    String dagName = "dag_name_1";
    String taskName = "task_name_1";
    String appName = "spark_app_name_1";
    logDao.updateJobInfo(
        "submission3",
        startTimeStamp,
        driverCore,
        driverMemoryMb,
        executorInstances,
        dagName,
        taskName,
        appName);
    logDao.updateExecutorInfo("submission3", executorCore, executorMemoryMb);

    try (ResultSet resultSet = logDao.getJobInfoQuery("RUNNING", "all", 200, 7)) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(resultSet.getString("submission_id"), "submission3");
      Assert.assertEquals(resultSet.getTimestamp("start_time"), startTimeStamp);
      Assert.assertEquals(resultSet.getInt("driver_core"), driverCore);
      Assert.assertEquals(resultSet.getInt("driver_memory_mb"), driverMemoryMb);
      Assert.assertEquals(resultSet.getInt("number_executor"), executorInstances);
      Assert.assertEquals(resultSet.getInt("executor_core"), executorCore);
      Assert.assertEquals(resultSet.getInt("executor_memory_mb"), executorMemoryMb);
      Assert.assertFalse(resultSet.getString("request_body").contains("***"));
      Assert.assertFalse(resultSet.next());
    }

    try (ResultSet resultSet =
        logDao.dbQuery(
            String.format(
                "select * from %s.application_submission where status='RUNNING'",
                DEFAULT_DB_NAME))) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(resultSet.getString("submission_id"), "submission3");
      Assert.assertEquals(resultSet.getTimestamp("start_time"), startTimeStamp);
      Assert.assertEquals(resultSet.getInt("driver_core"), driverCore);
      Assert.assertEquals(resultSet.getInt("driver_memory_mb"), driverMemoryMb);
      Assert.assertEquals(resultSet.getInt("number_executor"), executorInstances);
      Assert.assertEquals(resultSet.getInt("executor_core"), executorCore);
      Assert.assertEquals(resultSet.getInt("executor_memory_mb"), executorMemoryMb);
      Assert.assertFalse(resultSet.getString("request_body").contains("***"));
      Assert.assertFalse(resultSet.next());
    }

    logDao.logApplicationFinished(
        "submission3",
        "COMPLETED",
        new Timestamp(System.currentTimeMillis()),
        vCoreSecondCost,
        memoryMbSecondCost);

    try (ResultSet resultSet = logDao.getJobInfoQuery("COMPLETED", "all", 200, 7)) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(resultSet.getString("submission_id"), "submission3");
      Assert.assertEquals(resultSet.getTimestamp("start_time"), startTimeStamp);
      Assert.assertFalse(resultSet.next());
    }

    // the following log status code to revert to RUNNING status should not take effect since the
    // application is already completed
    logDao.logApplicationStatus("submission3", "RUNNING");
    try (ResultSet resultSet = logDao.getJobInfoQuery("RUNNING", "all", 200, 7)) {
      Assert.assertFalse(resultSet.next());
    }

    logDao.logApplicationFinished(
        "submission4",
        "COMPLETED",
        new Timestamp(System.currentTimeMillis()),
        vCoreSecondCost,
        memoryMbSecondCost);
    logDao.logApplicationFinished(
        "submission4",
        "COMPLETED",
        new Timestamp(System.currentTimeMillis()),
        vCoreSecondCost,
        memoryMbSecondCost);

    Assert.assertEquals(logDao.getSubmissionIdFromAppId("app1"), "submission1");

    ResultSet resultSet = logDao.getJobInfoQuery("COMPLETED", "all", 200, 7);
    int size = 0;
    while (resultSet.next()) {
      size++;
    }
    Assert.assertEquals(size, 2);

    insertToLogIndex(
        dbConnection,
        "logkey_2",
        "2000-01-01",
        "01",
        "c08-6b11cae7c6d747f7a1bdccb0b435eedc-exec-64");
    insertToLogIndex(
        dbConnection,
        "logkey_0",
        "2000-01-01",
        "01",
        "c08-6b11cae7c6d747f7a1bdccb0b435eedc-exec-64");
    insertToLogIndex(
        dbConnection,
        "logkey_1",
        "2000-01-01",
        "01",
        "c08-6b11cae7c6d747f7a1bdccb0b435eedc-exec-64");
    insertToLogIndex(
        dbConnection,
        "logkey_0",
        "2000-01-01",
        "01",
        "c08-6b11cae7c6d747f7a1bdccb0b435eedc-exec-64");

    ArrayList<String> keyList = logDao.getKeyList("c08-6b11cae7c6d747f7a1bdccb0b435eedc", "64");

    Assert.assertEquals(keyList.size(), 3);
    Assert.assertEquals(keyList.get(0), "logkey_0");
    Assert.assertEquals(keyList.get(1), "logkey_1");
    Assert.assertEquals(keyList.get(2), "logkey_2");
  }

  @Test
  public void testQueueTokenMasked() throws Exception {
    Class.forName(H2_DRIVER_CLASS);

    File file = File.createTempFile("h2_test_db", ".h2db");
    file.deleteOnExit();

    String connectionString =
        String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MySQL", file.getAbsolutePath());

    LogDao logDao = new LogDao(connectionString, null, null, null);
    Assert.assertNotNull(logDao);

    SubmitApplicationRequest submission = new SubmitApplicationRequest();
    submission.setSparkVersion("2.4");
    submission.setQueueToken("token1");
    logDao.logApplicationSubmission("submission1", "user1", submission);
    Assert.assertEquals(submission.getQueueToken(), "token1");

    logDao.logApplicationStatus("submission1", "RUNNING");

    try (ResultSet resultSet = logDao.getJobInfoQuery("RUNNING", "all", 200, 7)) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(resultSet.getString("submission_id"), "submission1");
      String requestBody = resultSet.getString("request_body");
      Assert.assertTrue(requestBody.contains("***"));
      Assert.assertFalse(requestBody.contains("token1"));
      Assert.assertFalse(resultSet.next());
    }
  }

  private void insertToLogIndex(
      DBConnection dbConnection, String key, String date, String hour, String containerId) {
    String sql =
        String.format(
            "INSERT IGNORE INTO %s.logindex VALUES ('%s', '%s', '%s', '%s')",
            DEFAULT_DB_NAME, key, date, hour, containerId);
    try (PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql)) {
      statement.executeUpdate();
    } catch (Throwable ex) {
      System.out.println(ex.getMessage());
    }
  }

  @Test
  public void testVerifyLegitDBName() {
    LogDao.verifyDBName("dummy");
  }

  @Test(
      expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "DB name should not be null or empty")
  public void testVerifyEmptyDBName() {
    LogDao.verifyDBName("");
  }

  @Test(expectedExceptions = {RuntimeException.class})
  public void testVerifyBadDBName() {
    LogDao.verifyDBName("dummy.");
  }
}
