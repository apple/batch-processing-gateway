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
import java.util.ArrayList;
import java.util.Arrays;
import javax.ws.rs.WebApplicationException;
import org.testng.annotations.Test;

public class QueueTokenVerifierTest {

  @Test
  public void verify_validToken() {
    String token =
        JwtUtils.createToken(
            "secret1",
            QueueTokenVerifier.ISSUER_ADMIN,
            "queueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            new String[] {"queue1"});
    QueueTokenVerifier.verify(token, Arrays.asList("secret1"), "queue1");

    token =
        JwtUtils.createToken(
            "secret1",
            QueueTokenVerifier.ISSUER_ADMIN,
            "queueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            new String[] {"queue1"});
    QueueTokenVerifier.verify(token, Arrays.asList("invalidSecret", "secret1"), "queue1");
  }

  @Test
  public void verify_validToken_multiQueuesInToken() {
    String token =
        JwtUtils.createToken(
            "secret1",
            QueueTokenVerifier.ISSUER_ADMIN,
            "queueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            new String[] {"qa", "qb", "queue1"});
    QueueTokenVerifier.verify(token, Arrays.asList("secret1"), "queue1");

    token =
        JwtUtils.createToken(
            "secret1",
            QueueTokenVerifier.ISSUER_ADMIN,
            "queueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            new String[] {"qa", "qb", "queue1"});
    QueueTokenVerifier.verify(token, Arrays.asList("invalidSecret", "secret1"), "queue1");
  }

  @Test
  public void verify_validToken_multiQueuesAsListInToken() {
    String token =
        JwtUtils.createToken(
            "secret1",
            QueueTokenVerifier.ISSUER_ADMIN,
            "queueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            Arrays.asList("qa", "qb", "queue1"));
    QueueTokenVerifier.verify(token, Arrays.asList("secret1"), "queue1");

    token =
        JwtUtils.createToken(
            "secret1",
            QueueTokenVerifier.ISSUER_ADMIN,
            "queueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            Arrays.asList("qa", "qb", "queue1"));
    QueueTokenVerifier.verify(token, Arrays.asList("invalidSecret", "secret1"), "queue1");
  }

  @Test
  public void verify_twoSecretsInVerify() {
    String token =
        JwtUtils.createToken(
            "secret1",
            QueueTokenVerifier.ISSUER_AIRFLOW,
            "queueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            new String[] {"queue1"});
    QueueTokenVerifier.verify(token, Arrays.asList("invalidSecret", "secret1"), "queue1");
  }

  @Test(expectedExceptions = WebApplicationException.class)
  public void verify_invalidSecret() {
    String token =
        JwtUtils.createToken(
            "secret1",
            QueueTokenVerifier.ISSUER_ADMIN,
            "queueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            new String[] {"queue1"});
    QueueTokenVerifier.verify(token, Arrays.asList("invalidSecret"), "queue1");
  }

  @Test(expectedExceptions = WebApplicationException.class)
  public void verify_invalidIssuer() {
    String token =
        JwtUtils.createToken(
            "secret1",
            "invalidIssuer",
            "queueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            new String[] {"queue1"});
    QueueTokenVerifier.verify(token, Arrays.asList("secret1"), "queue1");
  }

  @Test(expectedExceptions = WebApplicationException.class)
  public void verify_noAllowedQueuesClaim() {
    String token =
        JwtUtils.createToken(
            "secret1",
            QueueTokenVerifier.ISSUER_ADMIN,
            "queueToken",
            "claim1",
            new String[] {"queue1"});
    QueueTokenVerifier.verify(token, Arrays.asList("secret1"), "queue1");
  }

  @Test(expectedExceptions = WebApplicationException.class)
  public void verify_emptyAllowedQueuesClaim() {
    String token =
        JwtUtils.createToken(
            "secret1",
            QueueTokenVerifier.ISSUER_ADMIN,
            "queueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            new String[0]);
    QueueTokenVerifier.verify(token, Arrays.asList("secret1"), "queue1");
  }

  @Test(expectedExceptions = WebApplicationException.class)
  public void verify_emptyAllowedQueuesListClaim() {
    String token =
        JwtUtils.createToken(
            "secret1",
            QueueTokenVerifier.ISSUER_ADMIN,
            "queueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            new ArrayList<>());
    QueueTokenVerifier.verify(token, Arrays.asList("secret1"), "queue1");
  }
}
