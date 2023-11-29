package com.apple.spark.initContainer;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class VolumeMountInfo {

  private String name;
  private String mountPath;
  private String readOnly;
  private String subPath;
  private String subPathExpr;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getMountPath() {
    return mountPath;
  }

  public void setMountPath(String mountPath) {
    this.mountPath = mountPath;
  }

  public String getReadOnly() {
    return readOnly;
  }

  public void setReadOnly(String readOnly) {
    this.readOnly = readOnly;
  }

  public String getSubPath() {
    return subPath;
  }

  public void setSubPath(String subPath) {
    this.subPath = subPath;
  }

  public String getSubPathExpr() {
    return subPathExpr;
  }

  public void setSubPathExpr(String subPathExpr) {
    this.subPathExpr = subPathExpr;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (!(o instanceof VolumeMountInfo)) return false;

    VolumeMountInfo that = (VolumeMountInfo) o;

    return new EqualsBuilder()
        .append(getName(), that.getName())
        .append(getMountPath(), that.getMountPath())
        .append(getReadOnly(), that.getReadOnly())
        .append(getSubPath(), that.getSubPath())
        .append(getSubPathExpr(), that.getSubPathExpr())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(getName())
        .append(getMountPath())
        .append(getReadOnly())
        .append(getSubPath())
        .append(getSubPathExpr())
        .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(ToStringStyle.JSON_STYLE)
        .append("name", name)
        .append("mountPath", mountPath)
        .append("readOnly", readOnly)
        .append("subPath", subPath)
        .append("subPathExpr", subPathExpr)
        .toString();
  }
}
