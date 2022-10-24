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

package com.apple.spark.clients.sparkhistory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetJobEnvironmentResponse {

  private Map<String, String> runtime;
  private List<List<String>> sparkProperties;

  public List<List<String>> getSparkProperties() {
    return sparkProperties;
  }

  public void setSparkProperties(List<List<String>> sparkProperties) {
    this.sparkProperties = sparkProperties;
  }

  public Map<String, String> getRuntime() {
    return runtime;
  }

  public void setRuntime(Map<String, String> runtime) {
    this.runtime = runtime;
  }

  public String getPodNamePrefix() {
    String podNamePrefix = "";
    String key = "";
    String value = "";
    if (sparkProperties != null) {
      int n = this.sparkProperties.size();
      for (int i = 0; i < n; ++i) {
        List<String> item = sparkProperties.get(i);
        // the sparkProperties should have two items in each property
        if (item.size() == 2) {
          key = item.get(0);
          value = item.get(1);
          if (key.equalsIgnoreCase(
              "spark.kubernetes.executor.label.sparkoperator.k8s.io/app-name")) {
            podNamePrefix = value;
            break;
          }
        }
      }
    }
    return podNamePrefix;
  }
}
