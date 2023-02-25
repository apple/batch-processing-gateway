package com.apple.spark.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;

public class VirtualSparkClusterResource extends CustomResource implements Namespaced {
  private VirtualSparkClusterSpec spec;
  private VirtualSparkClusterStatus status;

  public VirtualSparkClusterSpec getSpec() {
    return spec;
  }

  public void setSpec(VirtualSparkClusterSpec spec) {
    this.spec = spec;
  }

  public VirtualSparkClusterStatus getStatus() {
    return status;
  }

  public void setStatus(VirtualSparkClusterStatus status) {
    this.status = status;
  }
}
