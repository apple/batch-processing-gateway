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

import java.util.Objects;

public class KubernetesClusterAndNamespace {

  private final String masterUrl;
  private final String namespace;

  public KubernetesClusterAndNamespace(String masterUrl, String namespace) {
    this.masterUrl = masterUrl;
    this.namespace = namespace;
  }

  public String getMasterUrl() {
    return masterUrl;
  }

  public String getNamespace() {
    return namespace;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KubernetesClusterAndNamespace that = (KubernetesClusterAndNamespace) o;
    return Objects.equals(masterUrl, that.masterUrl) && Objects.equals(namespace, that.namespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(masterUrl, namespace);
  }

  @Override
  public String toString() {
    return "KubernetesClusterAndNamespace{"
        + "masterUrl='"
        + masterUrl
        + '\''
        + ", namespace='"
        + namespace
        + '\''
        + '}';
  }
}
