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

import static com.apple.spark.core.Constants.ADMIN_API;

import com.apple.spark.AppConfig;
import com.apple.spark.core.Constants;
import com.apple.spark.core.KubernetesHelper;
import com.apple.spark.core.RestStreamingOutput;
import com.apple.spark.core.RestSubmissionsStreamingOutput;
import com.apple.spark.operator.SparkApplicationResourceList;
import com.apple.spark.security.User;
import com.apple.spark.util.ConfigUtil;
import com.apple.spark.util.ExceptionUtils;
import com.apple.spark.util.VersionInfo;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import java.io.OutputStream;
import javax.annotation.security.PermitAll;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PermitAll
@Path(ADMIN_API)
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
  @ApiResponse(
          responseCode = "200",
          content = @Content(mediaType = "application/json", schema = @Schema(type = "string")),
          description = "OK")
  public Response listSubmissions(
      @Parameter(hidden = true) @DefaultValue("none") @HeaderParam("Client-Version")
          String clientVersion,
      @Parameter(description = "specify this to list only submissions under one application name")
          @QueryParam("name")
          String applicationName,
      @Parameter(hidden = true) @Auth User user) {
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

  private Response listAllSubmissions(User user) {
    return Response.ok(
            new RestSubmissionsStreamingOutput() {
              @Override
              public void write(OutputStream outputStream) throws WebApplicationException {
                try {
                  for (AppConfig.SparkCluster sparkCluster : getSparkClusters()) {
                    SparkApplicationResourceList list = getSparkApplicationResources(sparkCluster);
                    writeSubmissions(outputStream, list, sparkCluster, getAppConfig());
                  }
                  logger.info(
                      "Finished streaming all submissions, requested by user {}", user.getName());
                } catch (Throwable ex) {
                  logger.warn(
                      String.format(
                          "Hit exception when streaming all submissions, requested by user %s",
                          user.getName()),
                      ex);
                  ExceptionUtils.meterException();
                }
              }
            })
        .build();
  }

  private Response listSubmissionsByApplicationName(String applicationName, User user) {
    return Response.ok(
            new RestSubmissionsStreamingOutput() {
              @Override
              public void write(OutputStream outputStream) throws WebApplicationException {
                try {
                  for (AppConfig.SparkCluster sparkCluster : getSparkClusters()) {
                    SparkApplicationResourceList list =
                        getSparkApplicationResourcesByLabel(
                            sparkCluster, Constants.APPLICATION_NAME_LABEL, applicationName);
                    writeSubmissions(outputStream, list, sparkCluster, getAppConfig());
                  }
                  logger.info(
                      "Finished streaming all submissions by application name,"
                          + " requested by user {}",
                      user.getName());
                } catch (Throwable ex) {
                  logger.warn(
                      String.format(
                          "Hit exception when streaming all submissions by application name,"
                              + " requested by user %s",
                          user.getName()),
                      ex);
                  ExceptionUtils.meterException();
                }
              }
            })
        .build();
  }

  @GET
  @Path("version")
  @Timed
  @Operation(
      summary = "Show the version",
      tags = {"Admin"})
  @ApiResponse(
          responseCode = "200",
          content = @Content(mediaType = "application/json", schema = @Schema(type = "string")),
          description = "OK")
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
