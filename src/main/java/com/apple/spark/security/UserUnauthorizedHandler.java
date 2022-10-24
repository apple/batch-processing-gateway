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

package com.apple.spark.security;

import com.google.common.collect.ImmutableMap;
import io.dropwizard.auth.UnauthorizedHandler;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

public class UserUnauthorizedHandler implements UnauthorizedHandler {

  private static final String CHALLENGE_FORMAT = "%s realm=\"%s\"";

  @Override
  public Response buildResponse(String prefix, String realm) {
    return Response.status(Response.Status.UNAUTHORIZED)
        .header(HttpHeaders.WWW_AUTHENTICATE, String.format(CHALLENGE_FORMAT, prefix, realm))
        .entity(ImmutableMap.of("code", "401", "message", "HTTP 401 Unauthorized."))
        .build();
  }
}
