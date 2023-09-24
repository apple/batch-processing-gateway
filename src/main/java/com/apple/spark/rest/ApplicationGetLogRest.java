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

import static com.apple.spark.core.Constants.LOG_API;
import static com.apple.spark.rest.AwsConstants.CLIENT_REGION;
import static com.apple.spark.rest.AwsConstants.S3_GET_TIMEOUT_MILLISECS;
import static com.apple.spark.util.HttpUtils.get;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.apple.spark.AppConfig;
import com.apple.spark.clients.sparkhistory.GetJobEnvironmentResponse;
import com.apple.spark.core.KubernetesHelper;
import com.apple.spark.core.LogDao;
import com.apple.spark.core.RestStreamingOutput;
import com.apple.spark.operator.SparkApplication;
import com.apple.spark.security.User;
import com.apple.spark.util.ExceptionUtils;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.auth.Auth;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.security.PermitAll;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PermitAll
@Path(LOG_API)
@Produces(MediaType.APPLICATION_JSON)
public class ApplicationGetLogRest extends RestBase {

  private static final Logger logger = LoggerFactory.getLogger(ApplicationGetLogRest.class);

  private static final String CANNOT_FIND_LOG_MESSAGE =
      "Cannot find matched log files. Please try again in 5 minutes if this is a new job which was"
          + " just started within 5 minutes.";

  private final MetricRegistry registry;
  private final LogDao logDao;

  public ApplicationGetLogRest(AppConfig appConfig, MeterRegistry meterRegistry) {
    super(appConfig, meterRegistry);
    this.registry = SharedMetricRegistries.getDefault();

    String dbConnectionString = null;
    String dbUser = null;
    String dbPassword = null;
    String dbName = null;

    if (appConfig.getDbStorageSOPS() != null) {
      dbConnectionString = appConfig.getDbStorageSOPS().getConnectionString();
      dbUser = appConfig.getDbStorageSOPS().getUser();
      dbPassword = appConfig.getDbStorageSOPS().getPasswordDecodedValue();
      dbName = appConfig.getDbStorageSOPS().getDbName();
    }

    this.logDao = new LogDao(dbConnectionString, dbUser, dbPassword, dbName, meterRegistry);
  }

  private AmazonS3 s3Client = getS3Client();

  @GET()
  @Timed
  @ExceptionMetered(
      name = "WebApplicationException",
      absolute = true,
      cause = WebApplicationException.class)
  @Operation(
      summary = "Get driver/executor stdout logs from EKS",
      description = "By default driver logs will be returned, unless executor ID is specified.",
      tags = {"Examination"})
  @ApiResponse(responseCode = "200", content = @Content(mediaType = "application/octet-stream"))
  @ApiResponse(
      responseCode = "400",
      description = "Bad request due to invalid submission ID, app ID or other issues")
  @ApiResponse(responseCode = "500", description = "Internal server error")
  public Response getLog(
      @Parameter(description = "submission ID (mutual exclusive with application ID)")
          @DefaultValue("")
          @QueryParam("subId")
          String submissionId,
      @Parameter(description = "application ID (mutual exclusive with submission ID)")
          @DefaultValue("")
          @QueryParam("appId")
          String appId,
      @Parameter(hidden = true) @DefaultValue("false") @QueryParam("s3only") String s3only,
      @Parameter(
              description =
                  "If execId is specified, logs from specific executor will be returned. Otherwise"
                      + " driver log will be returned.",
              example = "11")
          @DefaultValue("")
          @QueryParam("execId")
          String execId,
      @Parameter(hidden = true) @DefaultValue("none") @HeaderParam("Client-Version")
          String clientVersion,
      @Parameter(hidden = true) @Auth User user) {
    logger.info(
        "LogClientInfo: user {}, {}, Client-Version {}", user.getName(), "getLog", clientVersion);
    requestCounters.increment(
        REQUEST_METRIC_NAME, Tag.of("name", "get_log"), Tag.of("user", user.getName()));

    InputStream logStream = null;
    String errorMessage = "";
    ArrayList<String> objectKeys = new ArrayList<>();
    // UUID is 32 chars and 4 hyphens. Removes the hyphens, resulting in 32 chars.
    final int UUID_LENGTH_MIN = 32;

    if (appId.isEmpty() && submissionId.isEmpty()) {
      errorMessage = "Error: Please provide either a submissionId or an appId.";
      logger.error(errorMessage);
      throw new WebApplicationException(errorMessage, Response.Status.BAD_REQUEST);
      // Reject invalid submissionId or appId (if shorter than UUID_LENGTH_MIN)
    } else if ((!appId.isEmpty() && appId.length() < UUID_LENGTH_MIN)
        || (!submissionId.isEmpty() && submissionId.length() < UUID_LENGTH_MIN)) {
      errorMessage =
          String.format(
              "Invalid submissionId %s or appId %s : Should be at least 32 characters.",
              submissionId, appId);
      logger.error(errorMessage);
      throw new WebApplicationException(errorMessage, Response.Status.BAD_REQUEST);
    } else {
      String id = "";
      if (StringUtils.isEmpty(submissionId)) {
        // search rds database and get the submissionId
        id = getSubmissionIdFromAppIdFromDB(appId);
        // in case rds database does not have the record, search history server for submissionId
        if (StringUtils.isEmpty(id)) {
          id = getPodNamePrefixFromAppId(appId);
        }
      } else {
        id = submissionId;
      }

      if (StringUtils.isEmpty(id)) {
        errorMessage = String.format("Could not find submissionId from the appId %s.", appId);
        logger.error(errorMessage);
        throw new WebApplicationException(errorMessage, Response.Status.BAD_REQUEST);
      }

      // Try to get driver/executor logs from EKS first instead of S3.
      // If s3only is true, skip searching EKS.
      if (s3only.equalsIgnoreCase("false")) {
        try {
          final SparkApplication sparkApplicationResource = getSparkApplicationResource(id);
          logStream = getLog(sparkApplicationResource, execId);
        } catch (Throwable ex) {
          ExceptionUtils.meterException();
          logger.warn(String.format("Failed to get log stream for %s from EKS", id), ex);
        }
      }

      // If no logs found in EKS, search S3 bucket
      if (logStream == null) {
        logger.info("Searching RDS for subId: {} and execId: {}.", id, execId);
        objectKeys = logDao.getKeyList(id, execId);
      }
    }

    List<String> finalObjectKeys = objectKeys;
    InputStream logStreamFinal = logStream;

    return Response.ok(
            new RestStreamingOutput() {
              // The default buffer size is set to 8192 for now
              // to align with the default size of Java BufferedReader.
              // This should be configurable in the future.
              final byte[] buffer = new byte[8192];

              @Override
              public void write(OutputStream outputStream)
                  throws IOException, WebApplicationException {
                logger.info("Streaming log for submission {}", submissionId);

                if (finalObjectKeys.size() > 0) {
                  for (String objectKey : finalObjectKeys) {
                    logger.debug("Getting object key {}", objectKey);
                    try (S3Object s3Object =
                        s3Client.getObject(
                            new GetObjectRequest(
                                getAppConfig().getSparkLogS3Bucket(), objectKey))) {
                      InputStream objectStream = s3Object.getObjectContent();
                      BufferedReader objectStreamReader =
                          new BufferedReader(
                              new InputStreamReader(objectStream, StandardCharsets.UTF_8));

                      String line;
                      while ((line = objectStreamReader.readLine()) != null) {
                        // extract the log field out of the json formatted logs. The log field is
                        // between the \"log\":\" and \\n\",\"stream keywords
                        String formattedLine =
                            line.replaceAll(
                                ".*log\\\\\":\\\\\"(.*)\\\\\\\\n\\\\\",\\\\\"stream.*", "$1");
                        outputStream.write(formattedLine.getBytes(StandardCharsets.UTF_8));
                        outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
                      }

                      // add a newline at the end
                      outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
                    } catch (Exception e) {
                      logger.warn("Unable to download log object from S3: ", e);
                    }
                  }
                } else if (logStreamFinal != null) {
                  try {
                    int size = logStreamFinal.read(buffer);
                    while (size >= 0) {
                      outputStream.write(buffer, 0, size);
                      outputStream.flush();
                      size = logStreamFinal.read(buffer);
                    }
                    outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
                    logStreamFinal.close();
                    logger.info("Finished streaming log for submission {}", submissionId);
                  } catch (Throwable ex) {
                    ExceptionUtils.meterException();
                    logger.warn(
                        String.format(
                            "Hit exception when streaming log for submission %s", submissionId),
                        ex);
                    logStreamFinal.close();
                  }
                } else {
                  InputStream nothingMsg =
                      new ByteArrayInputStream(CANNOT_FIND_LOG_MESSAGE.getBytes());
                  int size = nothingMsg.read(buffer);
                  outputStream.write(buffer, 0, size);
                  outputStream.flush();
                }
              }
            })
        .build();
  }

  private AmazonS3 getS3Client() {
    if (s3Client == null) {
      ClientConfiguration clientConfiguration = new ClientConfiguration();
      clientConfiguration.setConnectionTimeout(S3_GET_TIMEOUT_MILLISECS);
      clientConfiguration.setRequestTimeout(S3_GET_TIMEOUT_MILLISECS);
      clientConfiguration.setSocketTimeout(S3_GET_TIMEOUT_MILLISECS);
      clientConfiguration.setClientExecutionTimeout(S3_GET_TIMEOUT_MILLISECS);

      s3Client =
          AmazonS3ClientBuilder.standard()
              .withRegion(CLIENT_REGION)
              .withClientConfiguration(clientConfiguration)
              .build();
    }
    return s3Client;
  }

  @ExceptionMetered(name = "RuntimeException", absolute = true, cause = RuntimeException.class)
  private InputStream getLog(SparkApplication sparkApplication, String execId) {
    if (sparkApplication == null) {
      logger.info("Cannot get log from EKS, spark application not found");
      return null;
    }
    String submissionId = sparkApplication.getMetadata().getName();
    AppConfig.SparkCluster sparkCluster = getSparkCluster(submissionId);
    DefaultKubernetesClient client = KubernetesHelper.getK8sClient(sparkCluster);
    String podName = "";
    try {
      if (sparkApplication.getStatus() == null) {
        logger.info("Cannot get log from EKS, spark application status empty for {}", submissionId);
        client.close();
        return null;
      }

      if (sparkApplication.getStatus().getDriverInfo() == null) {
        logger.info(
            "Cannot get log from EKS, spark application driver info empty for {}", submissionId);
        client.close();
        return null;
      }
      // execId by default is empty and which means only driver log is desired
      if (execId.isEmpty() || execId.endsWith("driver")) {
        podName = sparkApplication.getStatus().getDriverInfo().getPodName();
        // if execId is specified, then look for exec logs
      } else {
        Set<String> execPodNames = sparkApplication.getStatus().getExecutorState().keySet();
        for (String execPodName : execPodNames) {
          if (execPodName.endsWith("exec-" + execId)) {
            podName = execPodName;
          }
        }
      }
      if (podName == null || podName.isEmpty()) {
        logger.info("Cannot get log, spark application pod name empty for {}", submissionId);
        client.close();
        return null;
      }

      com.codahale.metrics.Timer timer =
          registry.timer(this.getClass().getSimpleName() + ".getLog.k8s-time");
      try (com.codahale.metrics.Timer.Context ignored = timer.time()) {
        InputStream logStream =
            KubernetesHelper.tryGetLogStream(
                client, sparkCluster.getSparkApplicationNamespace(), podName);
        if (logStream == null) {
          client.close();
          return null;
        }
        return logStream;
      }

    } catch (RuntimeException ex) {
      client.close();
      throw ex;
    }
  }

  private String getPodNamePrefixFromAppId(String appId) {
    String podNamePrefix = "";
    String response;
    GetJobEnvironmentResponse jobEnvionmentResponse = new GetJobEnvironmentResponse();
    String url =
        getAppConfig().getSparkHistoryUrl() + "/api/v1/applications/" + appId + "/environment";
    try {
      // SparkHistoryUrl is the ELB endpoint that EKS container can access
      // SparkHistoryDns is the actual sparkhisotry DNS that users use to access it
      response = get(url, "Host", getAppConfig().getSparkHistoryDns());
      ObjectMapper objectMapper = new ObjectMapper();
      jobEnvionmentResponse = objectMapper.readValue(response, GetJobEnvironmentResponse.class);
      podNamePrefix = jobEnvionmentResponse.getPodNamePrefix();
    } catch (Exception e) {
      logger.error(e.toString());
    }
    return podNamePrefix;
  }

  private String getSubmissionIdFromAppIdFromDB(String appId) {
    String submission_id = "";
    try {
      submission_id = logDao.getSubmissionIdFromAppId(appId);
    } catch (Exception e) {
      logger.warn(
          String.format("Could not get submission_id for appId: %s from database", appId), e);
    }
    return submission_id;
  }
}
