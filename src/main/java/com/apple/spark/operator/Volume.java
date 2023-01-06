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
import io.fabric8.kubernetes.api.model.CSIVolumeSource;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Volume {

  private String name;
  private HostPathVolumeSource hostPath;
  private ConfigMapVolumeSource configMap;
  private CSIVolumeSource csi;
  private EmptyDirVolumeSource emptyDir;

  public Volume() {
  }

  public Volume(String name, HostPathVolumeSource hostPath) {
    this.name = name;
    this.hostPath = hostPath;
  }

  public Volume(String name, ConfigMapVolumeSource configMap) {
    this.name = name;
    this.configMap = configMap;
  }

  public Volume(String name, CSIVolumeSource csi) {
    this.name = name;
    this.csi = csi;
  }

  public Volume(String name, EmptyDirVolumeSource emptyDir) {
    this.name = name;
    this.emptyDir = emptyDir;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public HostPathVolumeSource getHostPath() {
    return hostPath;
  }

  public void setHostPath(HostPathVolumeSource hostPath) {
    this.hostPath = hostPath;
  }

  public ConfigMapVolumeSource getConfigMap() {
    return configMap;
  }

  public void setConfigMap(ConfigMapVolumeSource configMap) {
    this.configMap = configMap;
  }

  public CSIVolumeSource getCsi() {
    return csi;
  }

  public void setCsi(CSIVolumeSource csi) {
    this.csi = csi;
  }

  public EmptyDirVolumeSource getEmptyDir() {
    return emptyDir;
  }

  public void setEmptyDir(EmptyDirVolumeSource emptyDir) {
    this.emptyDir = emptyDir;
  }
}
