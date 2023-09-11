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

import com.apple.spark.util.ExceptionUtils;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.jersey.errors.ErrorMessage;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import org.glassfish.jersey.server.ParamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThrowableExceptionMapper implements ExceptionMapper<Throwable> {

  private static final Logger logger = LoggerFactory.getLogger(ThrowableExceptionMapper.class);

  private final Meter exceptions;

  public ThrowableExceptionMapper(MetricRegistry metrics) {
    ParamException.QueryParamException a;
    exceptions = metrics.meter(MetricRegistry.name(getClass(), "exceptions"));
  }

  @Override
  public Response toResponse(Throwable throwable) {
    exceptions.mark();
    logger.warn(
        String.format("Hit exception: %s", ExceptionUtils.getExceptionNameAndMessage(throwable)),
        throwable);

    if (throwable instanceof WebApplicationException) {
      WebApplicationException webApplicationException = (WebApplicationException) throwable;
      return Response.status(webApplicationException.getResponse().getStatus())
          .type(MediaType.APPLICATION_JSON_TYPE)
          .entity(
              new ErrorMessage(
                  webApplicationException.getResponse().getStatus(),
                  ExceptionUtils.getExceptionNameAndMessage(webApplicationException)))
          .build();
    }

    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(
            new ErrorMessage(
                Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                ExceptionUtils.getExceptionNameAndMessage(throwable)))
        .build();
  }
}
