package com.apple.spark.crd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class VirtualSparkClusterStatus {
  private String state;

  public String getVirtualSparkClusterState() {
    return state;
  }

  public void setVirtualSparkClusterState(String state) {
    this.state = state;
  }
}
