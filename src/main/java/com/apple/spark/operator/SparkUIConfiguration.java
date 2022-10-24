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

package com.apple.spark.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkUIConfiguration {

  private Integer servicePort;
  private Map<String, String> ingressAnnotations;
  private List<IngressTLS> ingressTLS;

  public SparkUIConfiguration() {}

  public SparkUIConfiguration(SparkUIConfiguration configuration) {
    this.servicePort = configuration.servicePort;
    this.ingressAnnotations = null;
    if (configuration.ingressAnnotations != null) {
      this.ingressAnnotations = new HashMap<>(configuration.ingressAnnotations);
    }
    this.ingressTLS = null;
    if (configuration.ingressTLS != null) {
      this.ingressTLS = new ArrayList<>();
      for (IngressTLS entry : configuration.ingressTLS) {
        this.ingressTLS.add(new IngressTLS(entry));
      }
    }
  }

  public Integer getServicePort() {
    return servicePort;
  }

  public void setServicePort(Integer servicePort) {
    this.servicePort = servicePort;
  }

  public Map<String, String> getIngressAnnotations() {
    return ingressAnnotations;
  }

  public void setIngressAnnotations(Map<String, String> ingressAnnotations) {
    this.ingressAnnotations = ingressAnnotations;
  }

  public List<IngressTLS> getIngressTLS() {
    return ingressTLS;
  }

  public void setIngressTLS(List<IngressTLS> ingressTLS) {
    this.ingressTLS = ingressTLS;
  }
}
