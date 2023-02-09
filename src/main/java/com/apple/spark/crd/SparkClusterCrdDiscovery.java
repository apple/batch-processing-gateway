package com.apple.spark.crd;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class SparkClusterCrdDiscovery {
  private final AtomicReference<List<VirtualSparkClusterSpec>> clusters =
      new AtomicReference<>(getVirtualSparkClusterCrdSpec());

  public static List<VirtualSparkClusterSpec> getVirtualSparkClusterCrdSpec() {
    List<VirtualSparkClusterSpec> list =
        VirtualSparkClusterHelper.getVirtualSparkClusterConfigSpec();

    List<VirtualSparkClusterSpec> unmodifiedList = Collections.unmodifiableList(list);

    return unmodifiedList;
  }
}
