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

import com.apple.spark.api.GetMySubmissionsResponse;
import com.apple.spark.util.HttpUtils;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This tool could delete Spark applications by a given submission id.
// Please be careful when using this tool.
public class DeleteApplications {

  private static final Logger logger = LoggerFactory.getLogger(DeleteApplications.class);

  private static final String serviceRootUrl = ""; // update this before running
  private static final String getSubmissionsUrl =
      String.format("%s/spark/mySubmissions", serviceRootUrl);
  private static final String deleteUrlFormat = String.format("%s/spark/%%s", serviceRootUrl);
  private static final String authHeaderName = "Authorization";
  private static final String authHeaderValue = "Basic dXNlcjE6cGFzc3dvcmQ=";

  public static void main(String[] args) {
    String submissionIdSuffix = "loadtest";
    for (int i = 0; i < args.length; ) {
      String argName = args[i++];
      if (argName.equalsIgnoreCase("-submissionIdSuffix")) {
        submissionIdSuffix = args[i++];
      } else {
        throw new RuntimeException(String.format("Unsupported argument: %s", argName));
      }
    }

    GetMySubmissionsResponse getMySubmissionsResponse =
        HttpUtils.get(
            getSubmissionsUrl, authHeaderName, authHeaderValue, GetMySubmissionsResponse.class);

    final String submissionIdSuffixFinal = submissionIdSuffix;
    List<String> submissionIds =
        getMySubmissionsResponse.getSubmissions().stream()
            .filter(t -> t.getSubmissionId().endsWith(submissionIdSuffixFinal))
            .map(t -> t.getSubmissionId())
            .collect(Collectors.toList());

    final int totalCount = submissionIds.size();
    final AtomicInteger deletedIds = new AtomicInteger(0);
    submissionIds.parallelStream()
        .forEach(
            id -> {
              logger.info("Deleting submission {}", id);
              String deleteUrl = String.format(deleteUrlFormat, id);
              HttpUtils.delete(deleteUrl, authHeaderName, authHeaderValue);
              int currentDeletedCount = deletedIds.incrementAndGet();
              logger.info(
                  "Deleted submission {}, total deleted {} out of {}",
                  id,
                  currentDeletedCount,
                  totalCount);
            });

    System.exit(0);
  }
}
