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

package com.apple.spark.util;

import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.operator.SparkApplicationSpec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomSerDe {

  /**
   * Given an application submission request, serialize it to a JSON string with sensitive
   * information removed/masked
   *
   * @param submitRequest
   * @return a serialized JSON string
   * @throws JsonProcessingException
   */
  public static String submitRequestToNonSensitiveJson(SubmitApplicationRequest submitRequest)
      throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    SubmitApplicationRequest cloned =
        new ObjectMapper()
            .readValue(mapper.writeValueAsString(submitRequest), SubmitApplicationRequest.class);

    // remove the driver env vars
    if (cloned.getDriver() != null) {
      cloned.getDriver().setEnv(null);
    }

    // remove the executor env vars
    if (cloned.getExecutor() != null) {
      cloned.getExecutor().setEnv(null);
    }

    // mask the queue token
    if (cloned.getQueueToken() != null) {
      cloned.setQueueToken("***");
    }

    return mapper.writeValueAsString(cloned);
  }

  /**
   * Given a Spark application spec, serialize it to a JSON string with sensitive information
   * removed/masked
   *
   * @param sparkSpec
   * @return a serialized JSON string
   * @throws JsonProcessingException
   */
  public static String sparkSpecToNonSensitiveJson(SparkApplicationSpec sparkSpec)
      throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    SparkApplicationSpec cloned =
        new ObjectMapper()
            .readValue(mapper.writeValueAsString(sparkSpec), SparkApplicationSpec.class);

    // remove the driver env vars
    if (cloned.getDriver() != null) {
      cloned.getDriver().setEnv(null);
    }

    // remove the executor env vars
    if (cloned.getExecutor() != null) {
      cloned.getExecutor().setEnv(null);
    }

    return mapper.writeValueAsString(cloned);
  }
}
