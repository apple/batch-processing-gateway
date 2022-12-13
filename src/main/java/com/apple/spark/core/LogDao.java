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
import static com.apple.spark.core.SparkConstants.RUNNING_STATE;
import static com.apple.spark.core.SparkConstants.SUBMITTED_STATE;

import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.util.CounterMetricContainer;
import com.apple.spark.util.CustomSerDe;
import com.apple.spark.util.TimerMetricContainer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogDao {

  private static final Logger logger = LoggerFactory.getLogger(LogDao.class);

  private static final String DB_FAILURE_METRIC_NAME =
      String.format("statsd.%s.db.failure", Constants.SERVICE_ABBR);
  private static final String DB_TIMER_METRIC_NAME =
      String.format("statsd.%s.db.timer", Constants.SERVICE_ABBR);

  private static final String OPERATION_TAG = "operation";
  private static final String EXCEPTION_TAG = "exception";

  private static final String LOG_SUBMISSION_OPERATION = "log_submission";
  private static final String QUERY_SUBMISSION_OPERATION = "query_submission";
  private static final String QUERY_LOGINDEX_OPERATION = "query_logindex";
  private static final String INSERT_LOGINDEX_OPERATION = "insert_logindex";
  private static final String LOG_APP_ID_OPERATION = "log_app_id";
  private static final String LOG_APP_STATUS_OPERATION = "log_app_status";
  private static final String UPDATE_SPARK_APP_INFO_OPERATION = "update_spark_app_info";
  private static final String UPDATE_SPARK_EXECUTOR_INFO_OPERATION = "update_spark_executor_info";
  private static final String LOG_APP_FINISHED_OPERATION = "log_app_finished";

  private final DBConnection dbConnection;
  private final String dbName;

  private final CounterMetricContainer failureMetrics;
  private final TimerMetricContainer timerMetrics;

  private volatile boolean bypassLog = false;

  public LogDao(String connectionString, String user, String password, String dbName) {
    this(connectionString, user, password, dbName, new LoggingMeterRegistry());
  }

  public LogDao(
      String connectionString,
      String user,
      String password,
      String dbName,
      MeterRegistry meterRegistry) {
    if (StringUtils.isEmpty(dbName)) {
      dbName = DEFAULT_DB_NAME;
    }

    verifyDBName(dbName);

    if (StringUtils.isEmpty(connectionString)) {
      this.dbConnection = null;
      this.dbName = dbName;
      bypassLog = true;
    } else {
      this.dbConnection = new DBConnection(connectionString, user, password);
      this.dbName = dbName;
      createDatabase();
      createApplicationSubmissionTable();
      createLogIndexTable();
    }

    failureMetrics = new CounterMetricContainer(meterRegistry);
    timerMetrics = new TimerMetricContainer(meterRegistry);
  }

  /**
   * Verify a DB name to ensure the characters are legit: ASCII: [0-9,a-z,A-Z$_] (basic Latin
   * letters, digits 0-9, dollar, underscore) This is necessary for SQL queries.
   *
   * @param dbName
   */
  public static void verifyDBName(String dbName) {
    if (dbName == null || dbName.length() == 0) {
      throw new RuntimeException("DB name should not be null or empty");
    }
    if (dbName.matches(".*[^0-9a-zA-Z$_].*")) {
      throw new RuntimeException("DB name should include only [^0-9a-zA-Z$_]: " + dbName);
    }
  }

  public String getSubmissionIdFromAppId(String appId) {

    return timerMetrics.record(
        () -> getSubmissionIdFromAppIdImpl(appId),
        DB_TIMER_METRIC_NAME,
        Tag.of(OPERATION_TAG, QUERY_SUBMISSION_OPERATION));
  }

  // This method returns an empty string if the field value is null
  private String getSubmissionIdFromAppIdImpl(String appId) {
    String queryResult = "";

    String sql =
        String.format(
            "SELECT submission_id from %s.application_submission where app_id = ?", dbName);

    try (PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql)) {
      statement.setString(1, appId);
      try (ResultSet resultSet = statement.executeQuery()) {
        boolean seenResult = false;
        while (resultSet.next()) {
          // there must be one submission ID for any app ID
          if (seenResult) {
            logger.warn(String.format("Found multiple records for the same appId: %s"), appId);
            queryResult = "";
            break;
          }

          Object object = resultSet.getObject("submission_id");
          if (object == null) {
            queryResult = "";
          } else {
            queryResult = object.toString();
          }
          seenResult = true;
        }
      }
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to execute SQL query: %s", sql), ex);
      failureMetrics.increment(
          DB_FAILURE_METRIC_NAME,
          Tag.of(OPERATION_TAG, QUERY_SUBMISSION_OPERATION),
          Tag.of(EXCEPTION_TAG, ex.getClass().getSimpleName()));
    }
    return queryResult;
  }

  public void logApplicationSubmission(
      String submissionId, String user, SubmitApplicationRequest submission) {
    if (bypassLog) {
      return;
    }

    String requestBody = "";
    try {
      // when serializing for log, make sure the sensitive info is removed/masked
      requestBody = CustomSerDe.submitRequestToNonSensitiveJson(submission);
    } catch (Throwable ex) {
      logger.warn("Failed to serialize SubmitApplicationRequest and mask sensitive info", ex);
    }

    final SubmitApplicationRequest submissionCopy = submission;
    final String requestBodyCopy = requestBody;
    timerMetrics.record(
        () -> logApplicationSubmissionImpl(submissionId, user, submissionCopy, requestBodyCopy),
        DB_TIMER_METRIC_NAME,
        Tag.of(OPERATION_TAG, LOG_SUBMISSION_OPERATION));
  }

  private void logApplicationSubmissionImpl(
      String submissionId, String user, SubmitApplicationRequest submission, String requestBody) {
    String sql =
        String.format(
            "INSERT INTO %s.application_submission(submission_id, `user`, spark_version,"
                + " request_body,queue) VALUES (?, ?, ?, ?,?) ON DUPLICATE KEY UPDATE `user`=?,"
                + " spark_version=?, request_body=?, queue=?",
            dbName);

    logger.info("Executing SQL: {}", sql);
    try (PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql)) {
      statement.setString(1, submissionId);
      statement.setString(2, user);
      statement.setString(3, submission.getSparkVersion());
      statement.setString(4, requestBody);
      statement.setString(5, submission.getQueue());
      statement.setString(6, user);
      statement.setString(7, submission.getSparkVersion());
      statement.setString(8, requestBody);
      statement.setString(9, submission.getQueue());

      statement.executeUpdate();
    } catch (Throwable ex) {
      logger.warn(
          String.format(
              "Failed to log application submission %s, request body: %s, SQL: %s",
              submissionId, requestBody, sql),
          ex);
      failureMetrics.increment(
          DB_FAILURE_METRIC_NAME,
          Tag.of(OPERATION_TAG, LOG_SUBMISSION_OPERATION),
          Tag.of(EXCEPTION_TAG, ex.getClass().getSimpleName()));
    }
  }

  public void logApplicationId(String submissionId, String applicationId) {
    if (bypassLog) {
      return;
    }

    timerMetrics.record(
        () -> logApplicationIdImpl(submissionId, applicationId),
        DB_TIMER_METRIC_NAME,
        Tag.of(OPERATION_TAG, LOG_APP_ID_OPERATION));
  }

  private void logApplicationIdImpl(String submissionId, String applicationId) {
    String sql =
        String.format(
            "INSERT INTO %s.application_submission(submission_id, app_id)"
                + " VALUES (?, ?) ON DUPLICATE KEY UPDATE"
                + " app_id=?",
            dbName);

    logger.info("Executing SQL: {}", sql);
    try (PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql)) {
      statement.setString(1, submissionId);
      statement.setString(2, applicationId);
      statement.setString(3, applicationId);
      statement.executeUpdate();
    } catch (Throwable ex) {
      logger.warn(
          String.format(
              "Failed to log application id for submission %s, application id: %s, SQL: %s",
              submissionId, applicationId, sql),
          ex);
      failureMetrics.increment(
          DB_FAILURE_METRIC_NAME,
          Tag.of(OPERATION_TAG, LOG_APP_ID_OPERATION),
          Tag.of(EXCEPTION_TAG, ex.getClass().getSimpleName()));
    }
  }

  public void logApplicationStatus(String submissionId, String status) {
    if (bypassLog) {
      return;
    }

    timerMetrics.record(
        () -> logApplicationStatusImpl(submissionId, status),
        DB_TIMER_METRIC_NAME,
        Tag.of(OPERATION_TAG, LOG_APP_STATUS_OPERATION));
  }

  private void logApplicationStatusImpl(String submissionId, String status) {
    String sql =
        String.format(
            "UPDATE %s.application_submission"
                + " SET status=?"
                + " WHERE submission_id=? AND finished_time IS NULL",
            dbName);
    logger.info("Executing SQL: {}", sql);
    try (PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql)) {
      statement.setString(1, status);
      statement.setString(2, submissionId);
      statement.executeUpdate();
    } catch (Throwable ex) {
      logger.warn(
          String.format(
              "Failed to update application status for submission %s, status: %s, SQL: %s",
              submissionId, status, sql),
          ex);
      failureMetrics.increment(
          DB_FAILURE_METRIC_NAME,
          Tag.of(OPERATION_TAG, LOG_APP_STATUS_OPERATION),
          Tag.of(EXCEPTION_TAG, ex.getClass().getSimpleName()));
    }
  }

  public void logApplicationFinished(
      String submissionId,
      String status,
      Timestamp finishedTime,
      Double vCoreSecondCost,
      Double memoryMbSecondCost) {
    if (bypassLog) {
      return;
    }

    timerMetrics.record(
        () ->
            logApplicationFinishedImpl(
                submissionId, status, finishedTime, vCoreSecondCost, memoryMbSecondCost),
        DB_TIMER_METRIC_NAME,
        Tag.of(OPERATION_TAG, LOG_APP_FINISHED_OPERATION));
  }

  private void logApplicationFinishedImpl(
      String submissionId,
      String status,
      Timestamp finishedTime,
      Double vCoreSecondCost,
      Double memoryMbSecondCost) {
    String sql =
        String.format(
            "INSERT INTO %s.application_submission(submission_id, status, finished_time)"
                + " VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE"
                + " status=?,"
                + " finished_time=?,"
                + " cost=( (executor_memory_mb * number_executor + driver_memory_mb) * ? "
                + " + (executor_core * number_executor + driver_core) * ? "
                + ")"
                + " * (TIMESTAMPDIFF(second,start_time,finished_time))",
            dbName);
    logger.info("Executing SQL: {}", sql);
    try (PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql)) {
      statement.setString(1, submissionId);
      statement.setString(2, status);
      statement.setTimestamp(3, finishedTime);
      statement.setString(4, status);
      statement.setTimestamp(5, finishedTime);
      statement.setDouble(6, memoryMbSecondCost);
      statement.setDouble(7, vCoreSecondCost);
      statement.executeUpdate();
    } catch (Throwable ex) {
      logger.warn(
          String.format(
              "Failed to log application finished for submission %s, status: %s, SQL: %s",
              submissionId, status, sql),
          ex);
      failureMetrics.increment(
          DB_FAILURE_METRIC_NAME,
          Tag.of(OPERATION_TAG, LOG_APP_FINISHED_OPERATION),
          Tag.of(EXCEPTION_TAG, ex.getClass().getSimpleName()));
    }
  }

  private void createDatabase() {
    if (bypassLog) {
      return;
    }

    String sql = String.format("CREATE SCHEMA IF NOT EXISTS %s", dbName);
    try {
      dbConnection.executeSql(sql);
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to create database: %s", sql), ex);
      bypassLog = true;
    }
  }

  public boolean updateJobInfo(
      String submissionId,
      Timestamp startTime,
      int driverCore,
      int driverMemoryMB,
      int executorInstances,
      String dagName,
      String taskName,
      String appName) {
    if (bypassLog) {
      return true;
    }

    return timerMetrics.record(
        () ->
            updateJobInfoImpl(
                submissionId,
                startTime,
                driverCore,
                driverMemoryMB,
                executorInstances,
                dagName,
                taskName,
                appName),
        DB_TIMER_METRIC_NAME,
        Tag.of(OPERATION_TAG, UPDATE_SPARK_APP_INFO_OPERATION));
  }

  private boolean updateJobInfoImpl(
      String submissionId,
      Timestamp startTime,
      int driverCore,
      int driverMemoryMB,
      int executorInstances,
      String dagName,
      String taskName,
      String appName) {
    String sql =
        String.format(
            "UPDATE %s.application_submission"
                + " SET start_time=?,"
                + " driver_core=?,"
                + " driver_memory_mb=?,"
                + " number_executor=?,"
                + " dag_name=?,"
                + " task_name=?,"
                + " app_name=?"
                + " WHERE submission_id=? AND start_time IS NULL",
            dbName);
    logger.info("Executing SQL: {}", sql);
    try (PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql)) {
      statement.setTimestamp(1, startTime);
      statement.setInt(2, driverCore);
      statement.setInt(3, driverMemoryMB);
      statement.setInt(4, executorInstances);
      statement.setString(5, dagName);
      statement.setString(6, taskName);
      statement.setString(7, appName);
      statement.setString(8, submissionId);
      statement.executeUpdate();
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to update application info, SQL: %s", sql), ex);
      failureMetrics.increment(
          DB_FAILURE_METRIC_NAME,
          Tag.of(OPERATION_TAG, UPDATE_SPARK_APP_INFO_OPERATION),
          Tag.of(EXCEPTION_TAG, ex.getClass().getSimpleName()));
      return false;
    }
    return true;
  }

  private void createApplicationSubmissionTable() {
    if (bypassLog) {
      return;
    }

    String sql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.application_submission (\n"
                + "    submission_id VARCHAR(255) NOT NULL PRIMARY KEY,\n"
                + "    `user` VARCHAR(255) NULL,\n"
                + "    app_name VARCHAR(255) NULL,\n"
                + "    dag_name VARCHAR(255) NULL,\n"
                + "    task_name VARCHAR(255) NULL,\n"
                + "    spark_version VARCHAR(255) NULL,\n"
                + "    queue VARCHAR(255) NULL,\n"
                + "    status VARCHAR(255) NULL,\n"
                + "    app_id VARCHAR(255) NULL,\n"
                + "    request_body TEXT NULL,\n"
                + "    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
                + "    finished_time TIMESTAMP NULL,\n"
                + "    start_time TIMESTAMP NULL,\n"
                + "    driver_core SMALLINT,\n"
                + "    driver_memory_mb INT ,\n"
                + "    number_executor SMALLINT,\n"
                + "    executor_core SMALLINT,\n"
                + "    executor_memory_mb INT,\n"
                + "    cost DECIMAL(10,2)\n"
                + ");",
            dbName);
    try {
      dbConnection.executeSql(sql);
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to create table: %s", sql), ex);
      bypassLog = true;
    }
  }

  private void createLogIndexTable() {
    String sql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.logindex (\n"
                + "    logs3key VARCHAR(500) NOT NULL PRIMARY KEY,\n"
                + "    date date NOT NULL,\n"
                + "    \"hour\" SMALLINT NOT NULL,\n"
                + "    containerId VARCHAR(60) NOT NULL\n"
                + ");",
            dbName);
    try {
      dbConnection.executeSql(sql);
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to create table: %s", sql), ex);
    }
  }

  public ResultSet getJobInfoQuery(String status, String user, int queryLimit, int numDaysToShow) {
    return getJobInfoQueryImpl(status, user, queryLimit, numDaysToShow);
  }

  private ResultSet getJobInfoQueryImpl(
      String status, String user, int queryLimit, int numDaysToShow) {
    String sql = "";
    CachedRowSet crs = null;

    try {
      if (user.equals("all")) {
        if (status.equals(RUNNING_STATE) || status.equals(SUBMITTED_STATE)) {
          sql =
              String.format(
                  "SELECT *, TIMESTAMPDIFF(second,created_time,NOW()) AS duration FROM"
                      + " %s.application_submission WHERE created_time > (NOW() + CAST(? AS"
                      + " INTEGER) * INTERVAL '1' DAY) AND status = ? ORDER BY created_time DESC"
                      + " LIMIT ?",
                  dbName);
        } else {
          sql =
              String.format(
                  "SELECT *, TIMESTAMPDIFF(second,created_time,finished_time) AS duration FROM"
                      + " %s.application_submission WHERE created_time > (NOW() + CAST(? AS"
                      + " INTEGER) * INTERVAL '1' DAY) AND status = ? ORDER BY created_time DESC"
                      + " LIMIT ?",
                  dbName);
        }
        logger.info("Executing SQL: {}", sql);

        PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql);
        statement.setInt(1, -numDaysToShow);
        statement.setString(2, status);
        statement.setInt(3, queryLimit);

        ResultSet resultSet = statement.executeQuery();
        crs = RowSetProvider.newFactory().createCachedRowSet();
        crs.populate(resultSet);
      } else {
        if (status.equals(RUNNING_STATE) || status.equals(SUBMITTED_STATE)) {
          sql =
              String.format(
                  "SELECT *, TIMESTAMPDIFF(second,created_time,NOW()) AS duration FROM"
                      + " %s.application_submission WHERE created_time > (NOW() + CAST(? AS"
                      + " INTEGER) * INTERVAL '1' DAY) AND status = ? AND `user` = ? ORDER BY"
                      + " created_time DESC LIMIT ?",
                  dbName);
        } else {
          sql =
              String.format(
                  "SELECT *, TIMESTAMPDIFF(second,created_time,finished_time) AS duration FROM"
                      + " %s.application_submission WHERE created_time > (NOW() + CAST(? AS"
                      + " INTEGER) * INTERVAL '1' DAY) AND status = ? AND `user` = ? ORDER BY"
                      + " created_time DESC LIMIT ?",
                  dbName);
        }
        PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql);
        statement.setInt(1, -numDaysToShow);
        statement.setString(2, status);
        statement.setString(3, user);
        statement.setInt(4, queryLimit);

        logger.info("Executing SQL: {}", sql);

        ResultSet resultSet = statement.executeQuery();
        crs = RowSetProvider.newFactory().createCachedRowSet();
        crs.populate(resultSet);
      }
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to execute SQL query: %s", sql), ex);
      failureMetrics.increment(
          DB_FAILURE_METRIC_NAME,
          Tag.of(OPERATION_TAG, QUERY_SUBMISSION_OPERATION),
          Tag.of(EXCEPTION_TAG, ex.getClass().getSimpleName()));
    }

    return crs;
  }

  public boolean updateExecutorInfo(String submissionId, int executorCore, int executorMemoryMB) {
    if (bypassLog) {
      return true;
    }

    return timerMetrics.record(
        () -> updateExecutorInfoImpl(submissionId, executorCore, executorMemoryMB),
        DB_TIMER_METRIC_NAME,
        Tag.of(OPERATION_TAG, UPDATE_SPARK_EXECUTOR_INFO_OPERATION));
  }

  private boolean updateExecutorInfoImpl(
      String submissionId, int executorCore, int executorMemoryMB) {
    String sql =
        String.format(
            "UPDATE %s.application_submission"
                + " SET executor_core=?,"
                + " executor_memory_mb=?"
                + " WHERE submission_id=?",
            dbName);

    logger.info("Executing SQL: {}", sql);
    try (PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql)) {
      statement.setInt(1, executorCore);
      statement.setInt(2, executorMemoryMB);
      statement.setString(3, submissionId);
      statement.executeUpdate();
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to update executor info, SQL: %s", sql), ex);
      failureMetrics.increment(
          DB_FAILURE_METRIC_NAME,
          Tag.of(OPERATION_TAG, UPDATE_SPARK_EXECUTOR_INFO_OPERATION),
          Tag.of(EXCEPTION_TAG, ex.getClass().getSimpleName()));
      return false;
    }
    return true;
  }

  public ArrayList<String> getKeyList(String podPrefix, String execId) {
    return timerMetrics.record(
        () -> getKeyListImpl(podPrefix, execId),
        DB_TIMER_METRIC_NAME,
        Tag.of(OPERATION_TAG, QUERY_LOGINDEX_OPERATION));
  }

  // get log key list upon given a subId and execId(empty execId means driver)
  private ArrayList<String> getKeyListImpl(String podPrefix, String execId) {
    ArrayList<String> keyList = new ArrayList<String>();
    String containerId = "";
    String key = "";
    if (execId.isEmpty() || execId.endsWith("driver")) {
      containerId = podPrefix + "-driver";
    } else {
      containerId = podPrefix + "-exec-" + execId;
    }
    String sql =
        String.format(
            "SELECT logs3key from %s.logindex where containerId = ? ORDER BY logs3key", dbName);

    try (PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql)) {
      statement.setString(1, containerId);
      logger.info("Executing SQL: {}", sql);

      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          key = resultSet.getString(1);
          keyList.add(key);
        }
      }
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to execute SQL query: %s", sql), ex);
      failureMetrics.increment(
          DB_FAILURE_METRIC_NAME,
          Tag.of(OPERATION_TAG, QUERY_LOGINDEX_OPERATION),
          Tag.of(EXCEPTION_TAG, ex.getClass().getSimpleName()));
    }
    return keyList;
  }

  public ResultSet dbQuery(String sql) {
    return dbQueryImpl(sql);
  }

  private ResultSet dbQueryImpl(String sql) {

    logger.info("Executing SQL: {}", sql);
    CachedRowSet crs = null;
    try (PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql)) {
      ResultSet resultSet = statement.executeQuery();
      crs = RowSetProvider.newFactory().createCachedRowSet();
      crs.populate(resultSet);
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to execute SQL query: %s", sql), ex);
      failureMetrics.increment(
          DB_FAILURE_METRIC_NAME,
          Tag.of(OPERATION_TAG, QUERY_SUBMISSION_OPERATION),
          Tag.of(EXCEPTION_TAG, ex.getClass().getSimpleName()));
    }
    return crs;
  }
}
