package com.apple.spark.crd;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

/** This class is for retrieving Spark Cluster Resource from CRD */
public class VirtualSparkClusterResourceDoneable
    extends CustomResourceDoneable<VirtualSparkClusterResource> {
  public VirtualSparkClusterResourceDoneable(
      VirtualSparkClusterResource resource,
      Function<VirtualSparkClusterResource, VirtualSparkClusterResource> function) {
    super(resource, function);
  }
}
