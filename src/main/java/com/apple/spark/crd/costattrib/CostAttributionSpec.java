package com.apple.spark.crd.costattrib;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** This class is created to match Cost Attribution API. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CostAttributionSpec {
  private String createdTime;
  private String version;
  private String domain;
  private List<CostAttributionTag> tags;

  public String getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(String createdTime) {
    this.createdTime = createdTime;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getDomain() {
    return domain;
  }

  public void setDomain(String domain) {
    this.domain = domain;
  }

  public List<CostAttributionTag> getTags() {
    return tags;
  }

  public void setTags(List<CostAttributionTag> tags) {
    this.tags = tags;
  }
}
