package com.apple.spark.crd.costattrib;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

@Version("v1")
@Group("costattribution.k8s.io")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CostAttribution extends CustomResource implements Namespaced {
  private CostAttributionSpec spec;
  private CostAttributionStatus status;

  public CostAttributionSpec getSpec() {
    return spec;
  }

  public void setSpec(CostAttributionSpec spec) {
    this.spec = spec;
  }

  public CostAttributionStatus getStatus() {
    return status;
  }

  public void setStatus(CostAttributionStatus status) {
    this.status = status;
  }
}
