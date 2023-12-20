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
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodDNSConfig;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.swagger.v3.oas.annotations.Hidden;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkPodSpec {

  private Integer cores;
  private String coreLimit;
  private String coreRequest;
  private String memory;
  private String memoryOverhead;
  private GpuSpec gpu;
  private String image;
  private List<EnvVar> env;
  private Map<String, String> labels;
  private Map<String, String> annotations;

  @Hidden private Long terminationGracePeriodSeconds;
  @Hidden private String serviceAccount;
  @Hidden private List<VolumeMount> volumeMounts;
  @Hidden private List<InitContainer> gwInitContainers;
  @Hidden private List<Container> initContainers;
  @Hidden private SecurityContext securityContext;
  @Hidden private Affinity affinity;
  @Hidden private PodDNSConfig dnsConfig;

  @Hidden private String javaOptions;

  protected void copyFrom(SparkPodSpec another) {
    if (another.cores != null) {
      this.cores = another.cores;
    }
    if (another.coreLimit != null) {
      this.coreLimit = another.coreLimit;
    }
    if (another.coreRequest != null) {
      this.coreRequest = another.coreRequest;
    }
    if (another.memory != null) {
      this.memory = another.memory;
    }
    if (another.memoryOverhead != null) {
      this.memoryOverhead = another.memoryOverhead;
    }
    if (another.gpu != null) {
      this.gpu = another.gpu;
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

  public GpuSpec getGpu() {
    return gpu;
  }

  public void setGpu(GpuSpec gpu) {
    this.gpu = gpu;
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

  public List<InitContainer> getGwInitContainers() {
    return gwInitContainers;
  }

  public void setGwInitContainers(List<InitContainer> gwInitContainers) {
    this.gwInitContainers = gwInitContainers;
    if (!this.gwInitContainers.isEmpty()) {
      if (this.getInitContainers() == null) this.setInitContainers(new ArrayList<>());
      for (InitContainer gwInitContainer : this.getGwInitContainers()) {
        this.initContainers.add(convert2ContainerFromGWInitContainer(gwInitContainer));
      }
    }
  }

  public List<Container> getInitContainers() {
    return initContainers;
  }

  public void setInitContainers(List<Container> stdInitContainers) {
    this.initContainers = stdInitContainers;
  }

  public static List<InitContainer> convert2InitContainersFromFabric8(
      List<Container> stdInitContainers) {

    List<InitContainer> initContainers = new ArrayList<>();
    for (Container stdInitContainer : stdInitContainers) {
      initContainers.add(convert2InitContainerFromFabric8(stdInitContainer));
    }
    return initContainers;
  }

  private static InitContainer convert2InitContainerFromFabric8(Container stdContainer) {

    InitContainer currentInitContainer =
        new InitContainer(stdContainer.getName(), stdContainer.getImage());

    currentInitContainer.setEnv(convert2EnvVarFromFabric8(stdContainer.getEnv()));

    currentInitContainer.setSecurityContext(
        convert2SecurityContextFromFabric8(stdContainer.getSecurityContext()));
    currentInitContainer.setVolumeMounts(
        stdContainer.getVolumeMounts().stream()
            .map(SparkPodSpec::convert2VolumeMountFromFabric8)
            .collect(Collectors.toList()));

    currentInitContainer.setArgs(stdContainer.getArgs());

    return currentInitContainer;
  }

  private static List<EnvVar> convert2EnvVarFromFabric8(
      List<io.fabric8.kubernetes.api.model.EnvVar> ev) {

    return ev.stream()
        .map(
            e ->
                new EnvVar(
                    e.getName(),
                    e.getValue(),
                    e.getValueFrom() != null
                            && e.getValueFrom().getFieldRef() != null
                            && e.getValueFrom().getFieldRef().getFieldPath() != null
                        ? convert2EnvVarSourceFromFabric8(
                            new ObjectFieldSelector(e.getValueFrom().getFieldRef().getFieldPath()))
                        : null))
        .collect(Collectors.toList());
  }

  private static EnvVarSource convert2EnvVarSourceFromFabric8(
      ObjectFieldSelector objectFieldSelector) {

    EnvVarSource envVarSource = new EnvVarSource();
    envVarSource.setFieldRef(objectFieldSelector);

    return envVarSource;
  }

  private static SecurityContext convert2SecurityContextFromFabric8(
      io.fabric8.kubernetes.api.model.SecurityContext sc) {
    SecurityContext gatewaySC = new SecurityContext();
    gatewaySC.setRunAsGroup(sc.getRunAsGroup());
    gatewaySC.setRunAsUser(sc.getRunAsUser());
    return gatewaySC;
  }

  private static VolumeMount convert2VolumeMountFromFabric8(
      io.fabric8.kubernetes.api.model.VolumeMount vm) {
    VolumeMount gatewayVM = new VolumeMount(vm.getName(), vm.getMountPath());
    gatewayVM.setReadOnly(vm.getReadOnly());
    gatewayVM.setSubPath(vm.getSubPath());
    return gatewayVM;
  }

  public static Container convert2ContainerFromGWInitContainer(InitContainer gwInitContainer) {

    ContainerBuilder currentInitContainerFromGatewayVersionBuilder =
        new ContainerBuilder()
            .withName(gwInitContainer.getName())
            .withImage(gwInitContainer.getImage());

    if (gwInitContainer.getVolumeMounts() != null) {
      for (VolumeMount vm : gwInitContainer.getVolumeMounts()) {
        currentInitContainerFromGatewayVersionBuilder
            .addNewVolumeMount()
            .withName(vm.getName())
            .withMountPath(vm.getMountPath())
            .withReadOnly(vm.getReadOnly())
            .withSubPath(vm.getSubPath())
            .endVolumeMount();
      }
    }

    if (gwInitContainer.getEnv() != null) {
      for (EnvVar ev : gwInitContainer.getEnv()) {
        currentInitContainerFromGatewayVersionBuilder
            .addNewEnv()
            .withName(ev.getName())
            .withValue(ev.getValue())
            .withValueFrom(convert2EnvVarSourceFromGW(ev.getValueFrom()))
            .endEnv();
      }
    }

    if (gwInitContainer.getSecurityContext() != null) {

      currentInitContainerFromGatewayVersionBuilder.withSecurityContext(
          convert2SecurityContextFromGW(gwInitContainer.getSecurityContext()));
    }

    currentInitContainerFromGatewayVersionBuilder.withArgs(gwInitContainer.getArgs());

    return currentInitContainerFromGatewayVersionBuilder.build();
  }

  private static io.fabric8.kubernetes.api.model.EnvVarSource convert2EnvVarSourceFromGW(
      EnvVarSource evs) {

    if (evs != null) {
      io.fabric8.kubernetes.api.model.EnvVarSource envVarSource =
          new io.fabric8.kubernetes.api.model.EnvVarSource();

      io.fabric8.kubernetes.api.model.ObjectFieldSelector ofs =
          new io.fabric8.kubernetes.api.model.ObjectFieldSelector();
      ofs.setFieldPath(evs.getFieldRef().getFieldPath());
      envVarSource.setFieldRef(ofs);

      return envVarSource;
    }
    return null;
  }

  private static io.fabric8.kubernetes.api.model.SecurityContext convert2SecurityContextFromGW(
      SecurityContext sc) {

    SecurityContextBuilder securityContextBuilder = new SecurityContextBuilder();

    if (sc != null && sc.getRunAsUser() != null) {
      securityContextBuilder.withRunAsUser(sc.getRunAsUser());
    }
    if (sc != null && sc.getRunAsGroup() != null) {
      securityContextBuilder.withRunAsGroup(sc.getRunAsGroup());
    }
    if (sc != null && sc.getRunAsNonRoot() != null) {
      securityContextBuilder.withRunAsNonRoot(sc.getRunAsNonRoot());
    }
    if (sc != null && sc.getAllowPrivilegeEscalation() != null) {
      securityContextBuilder.withAllowPrivilegeEscalation(sc.getAllowPrivilegeEscalation());
    }
    return securityContextBuilder.build();
  }

  public static List<Container> convert2ContainersFromGWInitContainer(
      List<InitContainer> initContainers) {

    List<Container> stdContainers = new ArrayList<>();
    for (InitContainer initContainer : initContainers) {
      stdContainers.add(convert2ContainerFromGWInitContainer(initContainer));
    }
    return stdContainers;
  }
}
