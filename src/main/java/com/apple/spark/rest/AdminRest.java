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

package com.apple.spark.rest;

import static com.apple.spark.core.BatchSchedulerConstants.YUNIKORN_ROOT_QUEUE;
import static com.apple.spark.core.Constants.*;
import static org.apache.spark.network.util.JavaUtils.byteStringAsGb;

import com.apple.spark.AppConfig;
import com.apple.spark.api.GetJobsResponse;
import com.apple.spark.api.SubmissionStatus;
import com.apple.spark.api.SubmissionSummary;
import com.apple.spark.core.Constants;
import com.apple.spark.core.KubernetesHelper;
import com.apple.spark.core.LogDao;
import com.apple.spark.core.RestStreamingOutput;
import com.apple.spark.crd.VirtualSparkClusterSpec;
import com.apple.spark.operator.SparkApplication;
import com.apple.spark.operator.SparkApplicationResourceList;
import com.apple.spark.security.QueueAuthorizer;
import com.apple.spark.security.User;
import com.apple.spark.util.ConfigUtil;
import com.apple.spark.util.ExceptionUtils;
import com.apple.spark.util.VersionInfo;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.auth.Auth;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.security.PermitAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PermitAll
@Path(ADMIN_API)
@Produces(MediaType.APPLICATION_JSON)
public class AdminRest extends RestBase {
  private static final Logger logger = LoggerFactory.getLogger(AdminRest.class);
  private final LogDao logDao;
  private String dbName;
  private final Map<String, VirtualSparkClusterSpec> sparkClusterMap = new HashMap<>();
  private final List<VirtualSparkClusterSpec> sparkClusters;
  private final QueueAuthorizer queueAuthorizer;
  private final Map<String, AppConfig.QueueConfig> queueConfigs;

  public AdminRest(AppConfig appConfig, MeterRegistry meterRegistry) {

    super(appConfig, meterRegistry);

    String dbConnectionString = null;
    String dbUser = null;
    String dbPassword = null;

    if (appConfig.getDbStorageSOPS() != null) {
      dbConnectionString = appConfig.getDbStorageSOPS().getConnectionString();
      dbUser = appConfig.getDbStorageSOPS().getUser();
      dbPassword = appConfig.getDbStorageSOPS().getPasswordDecodedValue();
      dbName = appConfig.getDbStorageSOPS().getDbName();
    }

    this.logDao = new LogDao(dbConnectionString, dbUser, dbPassword, dbName, meterRegistry);

    this.sparkClusters = getSparkClusters();

    for (VirtualSparkClusterSpec sparkCluster : this.sparkClusters) {
      sparkClusterMap.put(sparkCluster.getId(), sparkCluster);
    }

    this.queueConfigs = appConfig.getQueueConfigs();
    AppConfig.Ranger ranger = appConfig.getRanger();

    if (ranger != null) {
      this.queueAuthorizer =
          new QueueAuthorizer(
              meterRegistry,
              queueConfigs,
              ranger.getSparkQueuePolicyRestUrl(),
              ranger.getSparkQueueXasecureAuditDestinationSolrUrls(),
              ranger.getUserGroupsCacheDurationInMillis());
    } else {
      logger.warn("queueAuthorizer is not enabled.");
      this.queueAuthorizer = null;
    }
  }

  @GET
  @Path("submissions")
  @Timed
  @Operation(
      summary = "List submissions from all users",
      tags = {"Admin"})
  @ApiResponse(responseCode = "200", content = @Content(mediaType = "application/octet-stream"))
  @ApiResponse(responseCode = "500", description = "Internal server error")
  public String listSubmissions(
      @Parameter(hidden = true) @DefaultValue("none") @HeaderParam("Client-Version")
          String clientVersion,
      @Parameter(description = "specify this to list only submissions under one application name")
          @QueryParam("name")
          String applicationName,
      @Parameter(hidden = true) @Auth User user)
      throws IOException, KubernetesClientException {
    requestCounters.increment(
        REQUEST_METRIC_NAME, Tag.of("name", ADMIN_SUBMISSIONS_TAG), Tag.of("user", user.getName()));

    checkRateForListSubmissions("submissions");
    logger.info(
        "LogClientInfo: user {}, {}, Client-Version {}",
        user.getName(),
        "listSubmissions",
        clientVersion);

    if (applicationName == null || applicationName.isEmpty()) {
      return listAllSubmissions(user);
    } else {
      applicationName = KubernetesHelper.normalizeLabelValue(applicationName);
      return listSubmissionsByApplicationName(applicationName, user);
    }
  }

  @GET
  @Path("jobsV1")
  @Timed
  @Operation(
      summary = "List job submissions from specific queue with filters",
      tags = {"Admin"})
  @ApiResponse(responseCode = "200", content = @Content(mediaType = "application/octet-stream"))
  @ApiResponse(responseCode = "500", description = "Internal server error")
  public String listJobs(
      @Parameter(hidden = true) @DefaultValue("none") @HeaderParam("Client-Version")
          String clientVersion,
      @Parameter(description = "Filter by queue", required = true) @QueryParam("queue")
          String queue,
      @Parameter(description = "Filter by submission id") @QueryParam("submission_id")
          String submissionId,
      @Parameter(description = "Filter by application id") @QueryParam("application_id")
          String applicationId,
      @Parameter(description = "Filter by application name") @QueryParam("application_name")
          String applicationName,
      @Parameter(description = "Filter by user") @QueryParam("user") String user,
      @Parameter(description = "Filter by airflow dag name") @QueryParam("airflow_dag_name")
          String airflowDagName,
      @Parameter(description = "Filter by status") @QueryParam("status") String status,
      @Parameter(description = "Filter by creation time") @QueryParam("creation_start_epoch_ms")
          String creationStartEpochMs,
      @Parameter(description = "Filter by creation time") @QueryParam("creation_end_epoch_ms")
          String creationEndEpochMs,
      @Parameter(description = "Filter by termination time")
          @QueryParam("termination_start_epoch_ms")
          String terminationStartEpochMs,
      @Parameter(description = "Filter by termination time") @QueryParam("termination_end_epoch_ms")
          String terminationEndEpochMs,
      @Parameter(
              description =
                  " Search airflow_dag_name, application_id, application_name,\n"
                      + "submission_id, and user.")
          @QueryParam("q")
          String q,
      @Parameter(description = "Sort the list by the specified column. Default: created_time")
          @QueryParam("sort")
          String sort,
      @Parameter(description = "Sort the data by specified sort column and order. Default: desc")
          @QueryParam("sort_order")
          String sortOrder,
      @Parameter(description = "Pagination, return this page.") @QueryParam("page") String page,
      @Parameter(description = "For pagination, return this many items per page")
          @QueryParam("per_page")
          String perPage)
      // TODO: Add @Auth User
      throws IOException, KubernetesClientException {

    logger.info("LogClientInfo: user {}, {}, Client-Version {}", "listJobs", clientVersion);

    if (isEmpty(queue)) {
      throw new WebApplicationException(
          String.format("Queue parameter is required"), Response.Status.BAD_REQUEST);
    }

    // Apply default value
    if (isEmpty(sort)) {
      sort = CREATION_TIME_LABEL;
    }

    if (isEmpty(sortOrder)) {
      sortOrder = DESC_SORT_ORDER;
    }

    if (isEmpty(page)) {
      page = "1";
    }

    if (isEmpty(perPage)) {
      perPage = "20";
    }

    int pageNum = Integer.parseInt(page);
    int perPageNum = Integer.parseInt(perPage);

    return listJobsImplV1(
        queue,
        submissionId,
        applicationId,
        applicationName,
        user,
        airflowDagName,
        status,
        creationStartEpochMs,
        creationEndEpochMs,
        terminationStartEpochMs,
        terminationEndEpochMs,
        q,
        sort,
        sortOrder,
        pageNum,
        perPageNum);
  }

  @GET
  @Path("jobs")
  @Timed
  @Operation(
      summary = "List job submissions with filters",
      tags = {"Admin"})
  @ApiResponse(responseCode = "200", content = @Content(mediaType = "application/octet-stream"))
  @ApiResponse(responseCode = "500", description = "Internal server error")
  public String listJobsV2(
      @Parameter(hidden = true) @DefaultValue("none") @HeaderParam("Client-Version")
          String clientVersion,
      @Parameter(description = "Filter by queue", required = true) @QueryParam("queue")
          String queue,
      @Parameter(description = "Filter by submission id") @QueryParam("submission_id")
          String submissionId,
      @Parameter(description = "Filter by application id") @QueryParam("application_id")
          String applicationId,
      @Parameter(description = "Filter by application name") @QueryParam("application_name")
          String applicationName,
      @Parameter(description = "Filter by user") @QueryParam("user") String user,
      @Parameter(description = "Filter by airflow dag name") @QueryParam("airflow_dag_name")
          String airflowDagName,
      @Parameter(description = "Filter by status") @QueryParam("status") String status,
      @Parameter(description = "Filter by creation time") @QueryParam("creation_start_epoch_ms")
          String creationStartEpochMs,
      @Parameter(description = "Filter by creation time") @QueryParam("creation_end_epoch_ms")
          String creationEndEpochMs,
      @Parameter(description = "Filter by termination time")
          @QueryParam("termination_start_epoch_ms")
          String terminationStartEpochMs,
      @Parameter(description = "Filter by termination time") @QueryParam("termination_end_epoch_ms")
          String terminationEndEpochMs,
      @Parameter(
              description =
                  " Search airflow_dag_name, application_id, application_name,\n"
                      + "submission_id, and user.")
          @QueryParam("q")
          String q,
      @Parameter(description = "Sort the list by the specified column. Default: created_time")
          @QueryParam("sort")
          String sort,
      @Parameter(description = "Sort the data by specified sort column and order. Default: desc")
          @QueryParam("sort_order")
          String sortOrder,
      @Parameter(description = "Returns the records starting from this offset") @QueryParam("start")
          String start,
      @Parameter(description = "Returns this number of records") @QueryParam("num") String num,
      @Auth User userRequested)
      throws IOException, KubernetesClientException {

    logger.info("LogClientInfo: user {}, {}, Client-Version {}", "listJobs", clientVersion);

    // Apply default value
    if (isEmpty(sort)) {
      sort = CREATED_TIME_COLUMN_NAME;
    }
    if (isEmpty(sortOrder)) {
      sortOrder = DESC_SORT_ORDER;
    }
    if (isEmpty(start)) {
      // Default should start from row 0, due to it's the first row according to Mysql syntax
      start = "0";
    }
    if (isEmpty(num)) {
      num = "200";
    }

    if (!SORTABLE_COLUMN_NAMES.contains(sort)) {
      throw new WebApplicationException(
          String.format(
              "Sort parameter must be one of sortable column names: %s, but it's: ",
              String.join(", ", SORTABLE_COLUMN_NAMES), sort),
          Response.Status.BAD_REQUEST);
    }
    if (!SORT_ORDERS.contains(sortOrder.toLowerCase())) {
      throw new WebApplicationException(
          String.format(
              "Sort order must be one of: %s, but it's: ",
              String.join(", ", SORT_ORDERS), sortOrder),
          Response.Status.BAD_REQUEST);
    }

    int startInt = Integer.parseInt(start);
    int numInt = Integer.parseInt(num);

    String finalSort = sort;
    String finalSortOrder = sortOrder;

    return timerMetrics.record(
        () ->
            listJobsImplV2(
                queue,
                submissionId,
                applicationId,
                applicationName,
                user,
                airflowDagName,
                status,
                creationStartEpochMs,
                creationEndEpochMs,
                terminationStartEpochMs,
                terminationEndEpochMs,
                q,
                finalSort,
                finalSortOrder,
                startInt,
                numInt,
                userRequested),
        LIST_JOBS_LATENCY_METRIC_NAME,
        Tag.of("name", LIST_JOBS_TAG),
        Tag.of("user", userRequested.getName()));
  }

  private boolean notEmpty(String field) {
    return (field != null) && (!field.isEmpty());
  }

  private boolean isEmpty(String field) {
    return field == null || field.isEmpty();
  }

  // Queue is mandatory
  private String listJobsImplV1(
      String queue,
      String submissionId,
      String applicationId,
      String applicationName,
      String user,
      String airflowDagName,
      String status,
      String creationStartEpochMs,
      String creationEndEpochMs,
      String terminationStartEpochMs,
      String terminationEndEpochMs,
      String q,
      String sort,
      String sortOrder,
      int page,
      int perPage)
      throws IOException, KubernetesClientException {
    try {
      List<SubmissionSummary> filteredSubmissions = new ArrayList<>();

      List<VirtualSparkClusterSpec> sparkClusters = getSparkClusters();
      logger.info(
          "All clusters are: "
              + String.join(
                  ", ",
                  sparkClusters.stream()
                      .map(sparkCluster -> sparkCluster.getEksCluster())
                      .collect(Collectors.toList())));

      if (!queue.contains(YUNIKORN_ROOT_QUEUE)) {
        throw new WebApplicationException(
            "Queue name " + queue + "doesn't start with " + YUNIKORN_ROOT_QUEUE,
            Response.Status.BAD_REQUEST);
      }

      // Queue name in gateway configuration doesn't include root. prefix
      String queueInBPGConf = queueWithoutRootPrefix(queue);
      List<VirtualSparkClusterSpec> queueOnClusters =
          sparkClusters.stream()
              .filter(sparkCluster -> sparkCluster.getQueues().contains(queueInBPGConf))
              .collect(Collectors.toList());
      logger.info(
          "Clusters that run queue "
              + queueInBPGConf
              + ": "
              + String.join(
                  ", ",
                  queueOnClusters.stream()
                      .map(sparkCluster -> sparkCluster.getEksCluster())
                      .collect(Collectors.toList())));

      for (VirtualSparkClusterSpec sparkCluster : queueOnClusters) {
        ArrayList<String> labelNames = new ArrayList<>();
        ArrayList<String> labelValues = new ArrayList<>();

        // Although we have filtered by the clusters that has specific queue's jobs, a cluster may
        // have other applications from different queues.
        // Putting it in labels parameter should be able to reduce # of records returned and boost
        // the speed
        labelNames.add(Constants.QUEUE_LABEL);
        labelValues.add(queue);

        if (notEmpty(submissionId)) {
          labelNames.add(Constants.AIML_OBSV_JOB_ID_LABEL);
          labelValues.add(submissionId);
        }
        if (notEmpty(applicationName)) {
          labelNames.add(Constants.APPLICATION_NAME_LABEL);
          labelValues.add(applicationName);
        }
        if (notEmpty(user)) {
          labelNames.add(Constants.PROXY_USER_LABEL);
          labelValues.add(user);
        }

        SparkApplicationResourceList list =
            getSparkApplicationResourcesByLabels(
                sparkCluster,
                labelNames.toArray(new String[labelNames.size()]),
                labelValues.toArray(new String[labelValues.size()]));

        List<SparkApplication> sparkApplicationResources = list.getItems();
        if (sparkApplicationResources == null) {
          continue;
        }
        logger.info(
            "Number of spark apps after initial filtering: " + sparkApplicationResources.size());

        for (SparkApplication sparkApplicationResource : sparkApplicationResources) {
          SubmissionSummary submission = new SubmissionSummary();
          submission.copyFrom(sparkApplicationResource, sparkCluster, appConfig);

          // All the filtering parameters except the ones already filtered with labels in above
          // (queue, submissionId, appName, user)
          if (filterSubmissionSummaryByFields(
              submission,
              applicationId,
              airflowDagName,
              status,
              creationStartEpochMs,
              creationEndEpochMs,
              terminationStartEpochMs,
              terminationEndEpochMs,
              q)) {
            filteredSubmissions.add(submission);
          }
        }
      }

      sortSubmissionsByField(filteredSubmissions, sort, sortOrder);

      return pagenationAndToString(filteredSubmissions, page, perPage);

    } catch (Throwable ex) {

      requestCounters.increment(
          REQUEST_ERROR_METRIC_NAME,
          Tag.of("exception", ex.getClass().getSimpleName()),
          Tag.of("name", ADMIN_SUBMISSIONS_TAG));

      throw ex;
    }
  }

  private String queueWithoutRootPrefix(String queue) {
    return queue.split(YUNIKORN_ROOT_QUEUE + ".")[1];
  }

  // When Ranger/queue auth is not enabled in this account, return all queues;
  // Otherwise, return the queues that either this user has list access, or queue auth is not
  // enabled on it
  private List<String> getAllAuthorizedQueues(User user) {
    return this.sparkClusters.stream()
        .flatMap(sparkCluster -> sparkCluster.getQueues().stream())
        .distinct()
        .filter(
            queue ->
                (queueAuthorizer == null)
                    || (!queueAuthorizer.authorizeEnabled(queue))
                    || (queueAuthorizer.isAuthorized(queue, "list", user.getName())))
        .collect(Collectors.toList());
  }

  // Parameters' validity is checked by the caller, this method assumes they are legal, although
  // some of them can be null
  private String listJobsImplV2(
      String queue,
      String submissionId,
      String applicationId,
      String applicationName,
      String user,
      String airflowDagName,
      String status,
      String creationStartEpochMs,
      String creationEndEpochMs,
      String terminationStartEpochMs,
      String terminationEndEpochMs,
      String q,
      String sort,
      String sortOrder,
      int start,
      int num,
      User userRequested)
      throws KubernetesClientException {
    try {

      requestCounters.increment(
          LIST_JOBS_REQUEST_METRIC_NAME,
          Tag.of("name", LIST_JOBS_TAG),
          Tag.of("user", userRequested.getName()));

      List<SubmissionSummary> filteredSubmissions = new ArrayList<>();

      StringBuilder query =
          new StringBuilder(
              // Only return records within the past 2 years
              String.format(
                  "SELECT * FROM %s.application_submission"
                      + " WHERE %s >= CURRENT_DATE - INTERVAL '2' YEAR",
                  dbName, CREATED_TIME_COLUMN_NAME));

      // Add filters related with queue authZ
      List<String> allAuthorizedQueues =
          getAllAuthorizedQueues(userRequested).stream()
              .map(qq -> String.format("queue = '%s'", qq))
              .collect(Collectors.toList());
      if (allAuthorizedQueues.size() != 0) {
        query.append(" AND (");
        query.append(String.join(" OR ", allAuthorizedQueues));
        query.append(")");
      }

      if (!isEmpty(queue)) {
        // We don't merge all the queue-related filters, for better readability, plus, the
        // optimization is basic for a db engine
        query.append(String.format(" AND queue = '%s'", queueWithoutRootPrefix(queue)));
      }
      if (!isEmpty(submissionId)) {
        query.append(String.format(" AND submission_id = '%s'", submissionId));
      }
      if (!isEmpty(applicationId)) {
        query.append(String.format(" AND app_id = '%s'", applicationId));
      }
      if (!isEmpty(applicationName)) {
        query.append(String.format(" AND app_name = '%s'", applicationName));
      }
      if (!isEmpty(user)) {
        query.append(String.format(" AND user = '%s'", user));
      }
      if (!isEmpty(airflowDagName)) {
        query.append(String.format(" AND dag_name = '%s'", airflowDagName));
      }
      if (!isEmpty(status)) {
        query.append(String.format(" AND status = '%s'", status));
      }
      if (!isEmpty(creationStartEpochMs)) {
        query.append(String.format(" AND created_time >= %s", creationStartEpochMs));
      }
      if (!isEmpty(creationEndEpochMs)) {
        query.append(String.format(" AND created_time <= %s", creationEndEpochMs));
      }
      if (!isEmpty(terminationStartEpochMs)) {
        query.append(String.format(" AND finished_time >= %s", terminationStartEpochMs));
      }
      if (!isEmpty(terminationEndEpochMs)) {
        query.append(String.format(" AND finished_time <= %s", terminationEndEpochMs));
      }

      if (!isEmpty(q)) {
        query.append(
            String.format(
                " AND ( dag_name like '%%%s%%'"
                    + " OR app_id like '%%%s%%'"
                    + " OR app_name like '%%%s%%'"
                    + " OR queue like '%%%s%%'"
                    + " OR submission_id like '%%%s%%'"
                    + " OR user like '%%%s%%'"
                    + ") ",
                q, q, q, q, q, q, q));
      }

      // Duration is not one column of the table, so the sort won't be done here, but after we get
      // the results from DB and have it set
      if (!sort.equals(DURATION_COLUMN_NAME)) {
        query.append(String.format(" ORDER BY %s", sort));
        query.append(String.format(" %s", sortOrder));
      }

      query.append(String.format(" LIMIT %s OFFSET %s", num, start));

      try (ResultSet resultSet =
          queryDBAndMeasureTime(
              query.toString(), LIST_JOBS_SQL_RESULTS_METRIC_NAME, LIST_JOBS_SQL_RESULTS_TAG)) {

        logger.info("Query for getting all eligible jobs: \n" + query + "\n");

        while (resultSet.next()) {

          String resultSetSubmissionId = resultSet.getString("submission_id");
          String resultSetUser = resultSet.getString("user");
          String resultSetSparkVersion = resultSet.getString("spark_version");
          String resultSetQueue = resultSet.getString("queue");
          String resultSetStatus = resultSet.getString("status");
          String resultSetAppId = resultSet.getString("app_id");
          Timestamp resultSetCreatedTime = resultSet.getTimestamp("created_time");
          Timestamp resultSetFinishedTime = resultSet.getTimestamp("finished_time");
          String resultSetAppName = resultSet.getString("app_name");
          String resultSetDagName = resultSet.getString("dag_name");
          int resultSetDriverCore = resultSet.getInt("driver_core");
          int resultSetDriverMemoryMb = resultSet.getInt("driver_memory_mb");
          int resultSetNumberExecutor = resultSet.getInt("number_executor");
          int resultSetExecutorCore = resultSet.getInt("executor_core");
          int resultSetExecutorMemoryMb = resultSet.getInt("executor_memory_mb");
          String resultSetArguments = resultSet.getString("arguments");
          // Two columns from the table are not used: start_time & task_name

          SubmissionSummary summary = new SubmissionSummary();

          summary.setSubmissionId(resultSetSubmissionId);
          summary.setUser(resultSetUser);
          summary.setSparkVersion(resultSetSparkVersion);
          summary.setQueue(YUNIKORN_ROOT_QUEUE + "." + resultSetQueue);
          summary.setApplicationState(resultSetStatus);
          summary.setSparkApplicationId(resultSetAppId);
          if (resultSetCreatedTime != null) {
            summary.setCreationTime(resultSetCreatedTime.getTime());
          }
          if (resultSetFinishedTime != null) {
            summary.setTerminationTime(resultSetFinishedTime.getTime());
          }
          summary.setApplicationName(resultSetAppName);
          summary.setDagName(resultSetDagName);
          summary.setDriverCores(resultSetDriverCore);
          summary.setExecutorInstances(resultSetNumberExecutor);
          summary.setExecutorCores(resultSetExecutorCore);

          if (resultSetArguments != null) {
            summary.setApplicationArguments(Arrays.asList(resultSetArguments.split(" ")));
          }

          // Set the following fields based on existing parameters
          summary.setDurationByStatus();
          summary.setAppStatusIfEmpty();

          summary.setDriverMemoryGB(byteStringAsGb(String.format("%sM", resultSetDriverMemoryMb)));
          summary.setExecutorMemoryGB(
              byteStringAsGb(String.format("%sM", resultSetExecutorMemoryMb)));

          // Set total cores and memory
          summary.setTotalResourcesBasedOnSpec();

          String clusterId = resultSetSubmissionId.split("-")[0];
          VirtualSparkClusterSpec sparkCluster = sparkClusterMap.get(clusterId);
          // It's possible that the specific Spark cluster that runs this Spark application is not
          // part of Skate's
          // sparkCluster configurations. This typically means the Spark cluster is already
          // decommissioned or not served
          // as one of the backend clusters for this gateway. Since this usually happens after a
          // long time the job
          // finished, and Spark eventLogs is nonetheless discarded after 10 days, we'll leave this
          // field empty in this
          // case
          if (sparkCluster != null) {
            summary.setSparkUIUrlBasedOnState(sparkCluster, appConfig);
          }

          summary.setAppMetricsUrlBasedOnConfig(appConfig);
          summary.setSplunkUrlBasedOnConfig(appConfig);

          filteredSubmissions.add(summary);
        }
      }

      // Sort the results separately if it's on duration, as this field is not in database and set
      // only after we retrieve the results
      if (sort.equals(DURATION_COLUMN_NAME)) {
        filteredSubmissions.sort(
            Comparator.comparing(
                SubmissionSummary::getDuration,
                sortOrder.equals(DESC_SORT_ORDER)
                    ? Comparator.reverseOrder()
                    : Comparator.naturalOrder()));
      }

      // We don't need to pass in num, due to the information is inside filteredSubmissions
      return toStringV2(filteredSubmissions, start);

    } catch (SQLException | IOException ex) {
      recordExceptionForJobs(ex, userRequested);
      logger.warn("Exception occurs in AdminRest/Jobs: " + ex.getMessage());
      ex.printStackTrace();
      throw new RuntimeException(ex);
    } catch (Throwable ex) {
      recordExceptionForJobs(ex, userRequested);
      throw ex;
    }
  }

  private ResultSet queryDBAndMeasureTime(String query, String metricName, String tagName) {

    return timerMetrics.record(() -> logDao.dbQuery(query), metricName, Tag.of("name", tagName));
  }

  private void recordExceptionForJobs(Throwable ex, User user) {
    requestCounters.increment(
        LIST_JOBS_ERROR_METRIC_NAME,
        Tag.of("exception", ex.getClass().getSimpleName()),
        Tag.of("name", LIST_JOBS_TAG),
        Tag.of("user", user.getName()));
  }

  private String pagenationAndToString(List<SubmissionSummary> submissions, int page, int perPage)
      throws IOException {

    GetJobsResponse jobs = new GetJobsResponse();

    jobs.set_page_info(Map.of("page", page, "per_page", perPage, "total", submissions.size()));
    List<SubmissionSummary> jobsInPage =
        submissions.stream().skip((page - 1) * perPage).limit(perPage).collect(Collectors.toList());
    jobs.setJobs(jobsInPage);

    ObjectMapper objectMapper = new ObjectMapper();
    StringBuilder submissionStr = new StringBuilder();

    submissionStr.append(objectMapper.writeValueAsString(jobs));
    return submissionStr.toString();
  }

  private String toStringV2(List<SubmissionSummary> submissions, int start) throws IOException {

    GetJobsResponse jobs = new GetJobsResponse();

    jobs.set_page_info(Map.of("start", start, "num", submissions.size()));
    jobs.setJobs(submissions);

    ObjectMapper objectMapper = new ObjectMapper();
    StringBuilder submissionStr = new StringBuilder();

    submissionStr.append(objectMapper.writeValueAsString(jobs));
    return submissionStr.toString();
  }

  private void sortSubmissionsByField(
      List<SubmissionSummary> submissions, String sort, String sortOrder) {

    Collections.sort(submissions, Comparator.comparingLong(SubmissionStatus::getCreationTime));

    if (sortOrder.equals(DESC_SORT_ORDER)) {
      Collections.reverse(submissions);
    }
  }

  private boolean filterSubmissionSummaryByFields(
      SubmissionSummary submission,
      String applicationId,
      String airflowDagName,
      String status,
      String creationStartEpochMs,
      String creationEndEpochMs,
      String terminationStartEpochMs,
      String terminationEndEpochMs,
      String q) {

    if (notEmpty(applicationId)) {
      if (!applicationId.equals(submission.getSparkApplicationId())) {
        return false;
      }
    }

    if (notEmpty(airflowDagName)) {
      if (!airflowDagName.equals(submission.getDagName())) {
        return false;
      }
    }

    if (notEmpty(status)) {
      if (!status.equals(submission.getApplicationState())) {
        return false;
      }
    }

    Long creationTime = submission.getCreationTime();
    if (creationTime != null) {
      if (notEmpty(creationStartEpochMs)) {
        if (creationTime < Long.parseLong(creationStartEpochMs)) {
          return false;
        }
      }
      if (notEmpty(creationEndEpochMs)) {
        if (creationTime > Long.parseLong(creationEndEpochMs)) {
          return false;
        }
      }
    }

    Long terminationTime = submission.getTerminationTime();
    if (terminationTime != null) {
      if (notEmpty(terminationStartEpochMs)) {
        if (terminationTime < Long.parseLong(terminationStartEpochMs)) {
          return false;
        }
      }
      if (notEmpty(terminationEndEpochMs)) {
        if (terminationTime > Long.parseLong(terminationEndEpochMs)) {
          return false;
        }
      }
    }

    // If search keyword exists
    if (notEmpty(q)) {

      String appAirflowDagName = submission.getDagName();
      String appApplicationId = submission.getSparkApplicationId();
      String appApplicationName = submission.getApplicationName();
      String appSubmissionId = submission.getSubmissionId();
      String appUser = submission.getUser();

      String[] searchableFields = {
        appAirflowDagName, appApplicationId, appApplicationName, appSubmissionId, appUser
      };
      for (String field : searchableFields) {
        if (field != null && field.contains(q)) {
          // Any one searchable field contains the keyword, the record is good
          return true;
        }
      }

      // If search keyword exists but no searchable field contains it, the record won't be in
      // results
      return false;
    }

    return true;
  }

  private String listAllSubmissions(User user) throws IOException, KubernetesClientException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      StringBuilder submissions = new StringBuilder();
      for (VirtualSparkClusterSpec sparkCluster : getSparkClusters()) {
        SparkApplicationResourceList list = getSparkApplicationResources(sparkCluster);
        List<SparkApplication> sparkApplicationResources = list.getItems();
        if (sparkApplicationResources == null) {
          continue;
        }
        for (SparkApplication sparkApplicationResource : sparkApplicationResources) {
          SubmissionSummary submission = new SubmissionSummary();
          submission.copyFrom(sparkApplicationResource, sparkCluster, appConfig);
          submissions.append(objectMapper.writeValueAsString(submission));
          submissions.append(System.lineSeparator());
        }
      }
      logger.info("Finished listing all submissions, requested by user {}", user.getName());
      return submissions.toString();
    } catch (Throwable ex) {
      requestCounters.increment(
          REQUEST_ERROR_METRIC_NAME,
          Tag.of("exception", ex.getClass().getSimpleName()),
          Tag.of("name", ADMIN_SUBMISSIONS_TAG),
          Tag.of("user", user.getName()));
      logger.warn(
          "Hit exception when listing all submissions, requested by user {}", user.getName());
      throw ex;
    }
  }

  private String listSubmissionsByApplicationName(String applicationName, User user)
      throws IOException, KubernetesClientException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      StringBuilder submissions = new StringBuilder();

      for (VirtualSparkClusterSpec sparkCluster : getSparkClusters()) {
        SparkApplicationResourceList list =
            getSparkApplicationResourcesByAppName(sparkCluster, applicationName);
        List<SparkApplication> sparkApplicationResources = list.getItems();
        if (sparkApplicationResources == null) {
          continue;
        }
        for (SparkApplication sparkApplicationResource : sparkApplicationResources) {
          SubmissionSummary submission = new SubmissionSummary();
          submission.copyFrom(sparkApplicationResource, sparkCluster, appConfig);
          submissions.append(objectMapper.writeValueAsString(submission));
          submissions.append(System.lineSeparator());
        }
      }
      logger.info(
          "Finished listing all submissions by application name {}, requested by user {}",
          applicationName,
          user.getName());
      return submissions.toString();
    } catch (Throwable ex) {
      requestCounters.increment(
          REQUEST_ERROR_METRIC_NAME,
          Tag.of("exception", ex.getClass().getSimpleName()),
          Tag.of("name", ADMIN_SUBMISSIONS_TAG),
          Tag.of("user", user.getName()),
          Tag.of("applicationName", applicationName));
      logger.warn(
          "Hit exception when listing all submissions by application name {}, requested by user {}",
          applicationName,
          user.getName());
      throw ex;
    }
  }

  private SparkApplicationResourceList getSparkApplicationResourcesByAppName(
      VirtualSparkClusterSpec sparkCluster, String applicationName) {

    String[] labelNames = new String[1];
    labelNames[0] = Constants.APPLICATION_NAME_LABEL;

    String[] labelValues = new String[1];
    labelValues[0] = applicationName;

    return getSparkApplicationResourcesByLabels(sparkCluster, labelNames, labelValues);
  }

  @GET
  @Path("version")
  @Timed
  @Operation(
      summary = "Show the version",
      tags = {"Admin"})
  @ApiResponse(responseCode = "200", content = @Content(mediaType = "application/octet-stream"))
  public Response version() {
    VersionInfo verInfo = ConfigUtil.readVersion();
    return Response.ok(
            new RestStreamingOutput() {
              @Override
              public void write(OutputStream outputStream) throws WebApplicationException {
                try {
                  ObjectMapper mapper = new ObjectMapper();
                  String verJson =
                      mapper.writerWithDefaultPrettyPrinter().writeValueAsString(verInfo);
                  writeLine(outputStream, verJson);

                  outputStream.flush();
                } catch (Throwable ex) {
                  logger.warn("Failed to get version info", ex);
                  ExceptionUtils.meterException();
                }
              }
            })
        .build();
  }

  @GET
  @Path("statuses")
  @Timed
  @Operation(
      summary = "Get all possible Spark job statuses",
      tags = {"Admin"})
  @ApiResponse(responseCode = "200", content = @Content(mediaType = "application/octet-stream"))
  public Response statuses() {

    return Response.ok(
            new RestStreamingOutput() {
              @Override
              public void write(OutputStream outputStream) throws WebApplicationException {
                try {
                  ObjectMapper mapper = new ObjectMapper();
                  String statusesJson =
                      mapper
                          .writerWithDefaultPrettyPrinter()
                          .writeValueAsString(Constants.STATUSES_MAP);
                  writeLine(outputStream, statusesJson);

                  outputStream.flush();
                } catch (Throwable ex) {
                  logger.warn("Failed to get statuses info", ex);
                  ExceptionUtils.meterException();
                }
              }
            })
        .build();
  }

  @GET
  @Path("queues")
  @Timed
  @Operation(
      summary = "Get all queues",
      tags = {"Admin"})
  @ApiResponse(responseCode = "200", content = @Content(mediaType = "application/octet-stream"))
  public Response queues(@Parameter(hidden = true) @Auth User user) {

    return Response.ok(
            new RestStreamingOutput() {
              @Override
              public void write(OutputStream outputStream) throws WebApplicationException {
                try {

                  Map<String, List<String>> queuesMap = new HashMap<>();

                  List<VirtualSparkClusterSpec> sparkClusters = getSparkClusters();

                  List<String> queues =
                      sparkClusters.stream()
                          .flatMap(sparkCluster -> sparkCluster.getQueues().stream())
                          .distinct()
                          .filter(
                              queue ->
                                  (queueAuthorizer == null)
                                      || (!queueAuthorizer.authorizeEnabled(queue))
                                      || (queueAuthorizer.isAuthorized(
                                          queue, "list", user.getName())))
                          // Queues configured in Skate don't contain a `root.` prefix
                          .map(queue -> YUNIKORN_ROOT_QUEUE + "." + queue)
                          .collect(Collectors.toList());

                  queuesMap.put("queues", queues);

                  ObjectMapper mapper = new ObjectMapper();
                  String queuesJson =
                      mapper.writerWithDefaultPrettyPrinter().writeValueAsString(queuesMap);
                  writeLine(outputStream, queuesJson);

                  outputStream.flush();
                } catch (Throwable ex) {
                  logger.warn("Failed to get queues info", ex);
                  ExceptionUtils.meterException();
                }
              }
            })
        .build();
  }
}
