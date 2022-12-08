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

import static com.apple.spark.core.Constants.ADMIN_API_V2;

import com.apple.spark.AppConfig;
import com.apple.spark.api.SubmissionSummary;
import com.apple.spark.core.Constants;
import com.apple.spark.core.KubernetesHelper;
import com.apple.spark.core.RestStreamingOutput;
import com.apple.spark.core.RestSubmissionsStreamingOutput;
import com.apple.spark.operator.SparkApplicationResource;
import com.apple.spark.operator.SparkApplicationResourceList;
import com.apple.spark.security.User;
import com.apple.spark.util.ConfigUtil;
import com.apple.spark.util.ExceptionUtils;
import com.apple.spark.util.VersionInfo;
import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.net.UnknownHostException;
import java.util.List;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PermitAll
@Path(ADMIN_API_V2)
@Produces(MediaType.APPLICATION_JSON)
public class AdminRest extends RestBase {

  private static final Logger logger = LoggerFactory.getLogger(AdminRest.class);

  public AdminRest(AppConfig appConfig, MeterRegistry meterRegistry) {
    super(appConfig, meterRegistry);
  }

  @GET
  @Path("submissions")
  @Timed
  @Operation(
      summary = "List submissions from all users",
      tags = {"Admin"})
  @ExceptionMetered(name = "SparkEKSClusterUnreachable", cause = UnknownHostException.class)
  @ApiResponse(responseCode = "200", content = @Content(mediaType = "application/octet-stream"))
  @ApiResponse(responseCode = "500", description = "Internal server error")
  public String listSubmissions(
      @Parameter(hidden = true) @DefaultValue("none") @HeaderParam("Client-Version")
          String clientVersion,
      @Parameter(description = "specify this to list only submissions under one application name")
          @QueryParam("name")
          String applicationName,
      @Parameter(hidden = true) @Auth User user) throws IOException {
    requestCounters.increment(
        REQUEST_METRIC_NAME, Tag.of("name", "admin_submissions"), Tag.of("user", user.getName()));

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

  private String listAllSubmissions(User user) throws IOException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      StringBuilder submissions = new StringBuilder();
      for (AppConfig.SparkCluster sparkCluster : getSparkClusters()) {
        SparkApplicationResourceList list = getSparkApplicationResources(sparkCluster);
        List<SparkApplicationResource> sparkApplicationResources = list.getItems();
        if (sparkApplicationResources == null) {
          continue;
        }
        for (SparkApplicationResource sparkApplicationResource : sparkApplicationResources) {
          SubmissionSummary submission = new SubmissionSummary();
          submission.copyFrom(sparkApplicationResource, sparkCluster, appConfig);
          submissions.append(objectMapper.writeValueAsString(submission));
          submissions.append(System.lineSeparator());
        }
      }
      logger.info("Finished listing all submissions, requested by user {}", user.getName());
      return submissions.toString();
    } catch (Throwable ex) {
      logger.warn("Hit exception when listing all submissions, requested by user {}", user.getName());
      ExceptionUtils.meterException();
      throw ex;
    }
  }

  private String listSubmissionsByApplicationName(
          String applicationName,
          User user) throws IOException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      StringBuilder submissions = new StringBuilder();
      for (AppConfig.SparkCluster sparkCluster : getSparkClusters()) {
        SparkApplicationResourceList list =
                getSparkApplicationResourcesByLabel(sparkCluster, Constants.APPLICATION_NAME_LABEL, applicationName);
        List<SparkApplicationResource> sparkApplicationResources = list.getItems();
        if (sparkApplicationResources == null) {
          continue;
        }
        for (SparkApplicationResource sparkApplicationResource : sparkApplicationResources) {
          SubmissionSummary submission = new SubmissionSummary();
          submission.copyFrom(sparkApplicationResource, sparkCluster, appConfig);
          submissions.append(objectMapper.writeValueAsString(submission));
          submissions.append(System.lineSeparator());
        }
      }
      logger.info(
              "Finished listing all submissions by application name {}, requested by user {}",
              applicationName, user.getName());
      return submissions.toString();
    } catch (Throwable ex) {
      logger.warn(
              "Hit exception when listing all submissions by application name {}, requested by user {}",
              applicationName, user.getName());
      ExceptionUtils.meterException();
      throw ex;
    }
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
}
