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

package com.apple.spark.tools;

import com.apple.spark.core.QueueTokenVerifier;
import com.apple.spark.util.JwtUtils;

/***
 * This tool generates a queue token with given arguments.
 * Argument example for this program: -secret secret1 -subject queueToken1 -queue queue1
 */
public class QueueTokenGenerator {

  public static void main(String[] args) {
    String secret = null;
    String subject = "queueToken";
    String queue = null;

    for (int i = 0; i < args.length; ) {
      String argName = args[i++];
      if (argName.equalsIgnoreCase("-secret")) {
        secret = args[i++];
      } else if (argName.equalsIgnoreCase("-subject")) {
        subject = args[i++];
      } else if (argName.equalsIgnoreCase("-queue")) {
        queue = args[i++];
      } else {
        System.err.println(String.format("Unsupported argument: %s", argName));
        printArgumentExample();
        System.exit(-1);
      }
    }

    if (secret == null || secret.isEmpty()) {
      System.err.println(String.format("Argument missing: -secret"));
      printArgumentExample();
      System.exit(-1);
    }

    if (subject == null || subject.isEmpty()) {
      System.err.println(String.format("Argument missing: -subject"));
      printArgumentExample();
      System.exit(-1);
    }

    if (queue == null || queue.isEmpty()) {
      System.err.println(String.format("Argument missing: -queue"));
      printArgumentExample();
      System.exit(-1);
    }

    String token =
        JwtUtils.createToken(
            secret,
            QueueTokenVerifier.ISSUER_ADMIN,
            subject,
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            new String[] {queue});
    System.out.println(token);
  }

  private static void printArgumentExample() {
    System.out.println(
        "Argument example for this program: -secret secret1 -subject queueToken1 -queue queue1");
  }
}
