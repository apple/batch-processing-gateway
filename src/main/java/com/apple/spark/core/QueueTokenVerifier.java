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

import com.apple.spark.util.JwtUtils;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueTokenVerifier {

  public static final String ISSUER_ADMIN = "admin";
  public static final String ISSUER_AIRFLOW = "airflow";
  public static final String CLAIM_ALLOWED_QUEUES = "allowedQueues";
  private static final Logger logger = LoggerFactory.getLogger(QueueTokenVerifier.class);

  // Verify whether the token is a valid queue token using given secrets to decode it.
  // Multiple decoding secrets here is to support secret rotation.
  // The method will also verify the queue token has a claim allowedQueues which contain the given
  // queue.
  public static void verify(String token, List<String> secretCandidates, String queue) {
    if (secretCandidates == null || secretCandidates.isEmpty()) {
      throw new WebApplicationException(
          "Server not configured with secrets to verify queue token",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    Throwable lastException = null;
    DecodedJWT jwt = null;

    for (int i = 0; i < secretCandidates.size(); i++) {
      try {
        String secret = secretCandidates.get(i);
        jwt = JwtUtils.verifyToken(token, secret);
      } catch (Throwable ex) {
        logger.warn(String.format("Failed to verify token with secret %s", i), ex);
        lastException = ex;
      }
    }

    if (jwt == null) {
      if (lastException == null || !(lastException instanceof JWTVerificationException)) {
        throw new WebApplicationException(
            "Server failed to verify queue token. Please contact service support.",
            Response.Status.INTERNAL_SERVER_ERROR);
      } else {
        throw new WebApplicationException(
            String.format(
                "Queue token is not valid: %s. Please provide a valid queue token, or contact"
                    + " service support.",
                lastException.getMessage()),
            Response.Status.BAD_REQUEST);
      }
    }

    String issuer = jwt.getIssuer();
    if (issuer == null || issuer.isEmpty()) {
      throw new WebApplicationException(
          "Queue token is not valid: no issuer. Please provide a valid queue token, or contact"
              + " service support.",
          Response.Status.BAD_REQUEST);
    }

    if (!issuer.equalsIgnoreCase(ISSUER_ADMIN) && !issuer.equalsIgnoreCase(ISSUER_AIRFLOW)) {
      throw new WebApplicationException(
          String.format(
              "Queue token is not valid due to unsupported issuer: %s. Please provide a valid queue"
                  + " token, or contact service support.",
              issuer),
          Response.Status.BAD_REQUEST);
    }

    Claim claim = jwt.getClaim(CLAIM_ALLOWED_QUEUES);
    if (claim == null || claim.isNull()) {
      throw new WebApplicationException(
          String.format(
              "Queue token is not valid due to no claim: %s. Please provide a valid queue token, or"
                  + " contact service support.",
              CLAIM_ALLOWED_QUEUES),
          Response.Status.BAD_REQUEST);
    }

    Set<String> allowedQueues = new HashSet<>();

    try {
      String[] values = claim.asArray(String.class);
      for (String str : values) {
        allowedQueues.add(str);
      }
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to convert claim %s to array", claim), ex);
    }

    if (allowedQueues.isEmpty()) {
      try {
        List<String> values = claim.asList(String.class);
        allowedQueues.addAll(values);
      } catch (Throwable ex) {
        logger.warn(String.format("Failed to convert claim %s to list", claim), ex);
      }
    }

    if (allowedQueues.isEmpty()) {
      throw new WebApplicationException(
          "Queue token has no allowed queues. Please provide a valid queue token, or contact"
              + " service support.",
          Response.Status.BAD_REQUEST);
    }

    if (!allowedQueues.contains(queue)) {
      throw new WebApplicationException(
          String.format(
              "Queue token does not allow queue %s. Please provide a valid queue token, or contact"
                  + " service support.",
              queue),
          Response.Status.BAD_REQUEST);
    }
  }
}
