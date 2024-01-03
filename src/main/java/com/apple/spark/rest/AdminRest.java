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

import com.apple.spark.AppConfig;
import com.apple.spark.api.GetJobsResponse;
import com.apple.spark.api.SubmissionStatus;
import com.apple.spark.api.SubmissionSummary;
import com.apple.spark.core.Constants;
import com.apple.spark.core.KubernetesHelper;
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
  private final QueueAuthorizer queueAuthorizer;
  private final Map<String, AppConfig.QueueConfig> queueConfigs;

  public AdminRest(AppConfig appConfig, MeterRegistry meterRegistry) {
    super(appConfig, meterRegistry);

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
  @Path("jobs")
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
      @Parameter(description = "Sort the list by the specified column. Default: creation_time")
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
    requestCounters.increment(REQUEST_METRIC_NAME, Tag.of("name", ADMIN_SUBMISSIONS_TAG));

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

    return listJobsImpl(
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

  private boolean notEmpty(String field) {
    return (field != null) && (!field.isEmpty());
  }

  private boolean isEmpty(String field) {
    return field == null || field.isEmpty();
  }

  // Queue is mandatory
  private String listJobsImpl(
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
      String queueInBPGConf = queue.split(YUNIKORN_ROOT_QUEUE + ".")[1];
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
