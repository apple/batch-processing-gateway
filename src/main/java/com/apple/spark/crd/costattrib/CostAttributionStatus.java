package com.apple.spark.crd.costattrib;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CostAttributionStatus {
  private String state;

  public String getCostAttributionState() {
    return state;
  }

  public void setCostAttributionState(String state) {
    this.state = state;
  }
}
