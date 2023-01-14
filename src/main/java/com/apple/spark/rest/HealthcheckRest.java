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

import static com.apple.spark.core.Constants.HEALTH_CHECK_API;

import com.apple.spark.AppConfig;
import com.apple.spark.api.HealthcheckResponse;
import com.apple.spark.security.User;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import javax.annotation.security.PermitAll;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PermitAll
@Path(HEALTH_CHECK_API)
@Produces(MediaType.APPLICATION_JSON)
public class HealthcheckRest extends RestBase {

  private static final Logger logger = LoggerFactory.getLogger(HealthcheckRest.class);

  public HealthcheckRest(AppConfig appConfig, MeterRegistry meterRegistry) {
    super(appConfig, meterRegistry);
  }

  @GET
  @Path("status")
  @Operation(
      summary = "Check the current service status",
      tags = {"Health Check"})
  @ApiResponse(
      responseCode = "200",
      content =
          @Content(
              mediaType = "application/json",
              schema = @Schema(implementation = HealthcheckResponse.class)))
  public HealthcheckResponse healthcheckStatus(@Parameter(hidden = true) @Auth User user) {
    requestCounters.increment(
        REQUEST_METRIC_NAME, Tag.of("name", "healthcheck"), Tag.of("user", user.getName()));
    HealthcheckResponse response = new HealthcheckResponse();
    response.setStatus("OK");
    return response;
  }
}
