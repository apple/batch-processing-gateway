package com.apple.spark.crd;

import static com.apple.spark.core.Constants.*;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SparkClusterCrdDiscovery {
  private static final SparkClusterCrdDiscovery sparkClusterCrdDiscovery =
      new SparkClusterCrdDiscovery();

  private static LoadingCache<String, List<VirtualSparkClusterSpec>> cache;

  public SparkClusterCrdDiscovery() {
    CacheLoader<String, List<VirtualSparkClusterSpec>> loader =
        new CacheLoader<>() {
          @Override
          public List<VirtualSparkClusterSpec> load(String key) {
            return Collections.unmodifiableList(
                VirtualSparkClusterHelper.getVirtualSparkClusterConfigSpec());
          }
        };

    cache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.SECONDS).build(loader);
  }

  public static SparkClusterCrdDiscovery getInstance() {
    return sparkClusterCrdDiscovery;
  }

  public List<VirtualSparkClusterSpec> getClusters() {
    return cache.getUnchecked("first");
  }
}
