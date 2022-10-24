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

import com.apple.spark.AppConfig;
import com.apple.spark.api.SubmissionSummary;
import com.apple.spark.operator.SparkApplicationResource;
import com.apple.spark.operator.SparkApplicationResourceList;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public abstract class RestSubmissionsStreamingOutput extends RestStreamingOutput {

  protected void writeSubmissions(
      OutputStream outputStream,
      SparkApplicationResourceList list,
      AppConfig.SparkCluster sparkCluster,
      AppConfig appConfig)
      throws IOException {
    if (list == null) {
      return;
    }
    List<SparkApplicationResource> sparkApplicationResources = list.getItems();
    if (sparkApplicationResources == null) {
      return;
    }
    ObjectMapper objectMapper = new ObjectMapper();
    for (SparkApplicationResource sparkApplicationResource : sparkApplicationResources) {
      SubmissionSummary submission = new SubmissionSummary();
      submission.copyFrom(sparkApplicationResource, sparkCluster, appConfig);
      String str = objectMapper.writeValueAsString(submission);
      writeLine(outputStream, str);
    }
    outputStream.flush();
  }
}
