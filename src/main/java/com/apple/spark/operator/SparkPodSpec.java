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
import io.fabric8.kubernetes.api.model.PodDNSConfig;
import io.swagger.v3.oas.annotations.Hidden;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkPodSpec {

  private Integer cores;
  private String coreLimit;
  private String memory;
  private String memoryOverhead;
  @Hidden private String image;
  private List<EnvVar> env;
  @Hidden private Map<String, String> labels;
  @Hidden private Map<String, String> annotations;
  @Hidden private Long terminationGracePeriodSeconds;
  @Hidden private String serviceAccount;
  @Hidden private List<VolumeMount> volumeMounts;

  @Hidden private SecurityContext securityContext;

  @Hidden private Affinity affinity;
  @Hidden private PodDNSConfig dnsConfig;

  @Hidden private String coreRequest;
  @Hidden private String javaOptions;

  protected void copyFrom(SparkPodSpec another) {
    if (another.cores != null) {
      this.cores = another.cores;
    }
    if (another.coreLimit != null) {
      this.coreLimit = another.coreLimit;
    }
    if (another.memory != null) {
      this.memory = another.memory;
    }
    if (another.memoryOverhead != null) {
      this.memoryOverhead = another.memoryOverhead;
    }
    if (another.image != null) {
      this.image = another.image;
    }
    if (another.env == null) {
      this.env = null;
    } else {
      this.env = new ArrayList<>(another.env);
    }
    if (another.labels == null) {
      this.labels = null;
    } else {
      this.labels = new HashMap<>(another.labels);
    }
    if (another.annotations == null) {
      this.annotations = null;
    } else {
      this.annotations = new HashMap<>(another.annotations);
    }
    this.terminationGracePeriodSeconds = another.terminationGracePeriodSeconds;
    this.serviceAccount = another.serviceAccount;
    if (another.volumeMounts == null) {
      this.volumeMounts = null;
    } else {
      this.volumeMounts = new ArrayList<>(another.volumeMounts);
    }
    this.securityContext = another.securityContext;
    this.affinity = another.affinity;

    this.coreRequest = another.coreRequest;
    this.javaOptions = another.javaOptions;
  }

  public Integer getCores() {
    return cores;
  }

  public void setCores(Integer cores) {
    this.cores = cores;
  }

  public String getCoreLimit() {
    return coreLimit;
  }

  public void setCoreLimit(String coreLimit) {
    this.coreLimit = coreLimit;
  }

  public String getMemory() {
    return memory;
  }

  public void setMemory(String memory) {
    this.memory = memory;
  }

  public String getMemoryOverhead() {
    return memoryOverhead;
  }

  public void setMemoryOverhead(String memoryOverhead) {
    this.memoryOverhead = memoryOverhead;
  }

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }

  public List<EnvVar> getEnv() {
    return env;
  }

  public void setEnv(List<EnvVar> env) {
    this.env = env;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public void setLabels(Map<String, String> labels) {
    this.labels = labels;
  }

  public Map<String, String> getAnnotations() {
    return annotations;
  }

  public void setAnnotations(Map<String, String> annotations) {
    this.annotations = annotations;
  }

  public Long getTerminationGracePeriodSeconds() {
    return terminationGracePeriodSeconds;
  }

  public void setTerminationGracePeriodSeconds(Long terminationGracePeriodSeconds) {
    this.terminationGracePeriodSeconds = terminationGracePeriodSeconds;
  }

  public PodDNSConfig getDnsConfig() {
    return dnsConfig;
  }

  public void setDnsConfig(PodDNSConfig dnsConfig) {
    this.dnsConfig = dnsConfig;
  }

  public String getServiceAccount() {
    return serviceAccount;
  }

  public void setServiceAccount(String serviceAccount) {
    this.serviceAccount = serviceAccount;
  }

  public List<VolumeMount> getVolumeMounts() {
    return volumeMounts;
  }

  public void setVolumeMounts(List<VolumeMount> volumeMounts) {
    this.volumeMounts = volumeMounts;
  }

  public SecurityContext getSecurityContext() {
    return securityContext;
  }

  public void setSecurityContext(SecurityContext securityContext) {
    this.securityContext = securityContext;
  }

  public Affinity getAffinity() {
    return affinity;
  }

  public void setAffinity(Affinity affinity) {
    this.affinity = affinity;
  }

  public String getCoreRequest() {
    return coreRequest;
  }

  public void setCoreRequest(String coreRequest) {
    this.coreRequest = coreRequest;
  }

  public String getJavaOptions() {
    return javaOptions;
  }

  public void setJavaOptions(String javaOptions) {
    this.javaOptions = javaOptions;
  }
}
