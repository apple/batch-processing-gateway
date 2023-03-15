package com.apple.spark.crd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

@Version("v1")
@Group("virtualsparkcluster.k8s.io")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class VirtualSparkCluster extends CustomResource implements Namespaced {
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
