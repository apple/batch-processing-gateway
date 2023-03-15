package com.apple.spark.crd;

import static com.apple.spark.core.Constants.*;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SparkClusterCrdDiscovery {
  private static final SparkClusterCrdDiscovery instance = new SparkClusterCrdDiscovery();

  private static LoadingCache<String, List<VirtualSparkClusterSpec>> cache;

  //    tryGetServiceAccountNamespace
  public SparkClusterCrdDiscovery() {
    CacheLoader<String, List<VirtualSparkClusterSpec>> loader =
        new CacheLoader<>() {
          @Override
          public List<VirtualSparkClusterSpec> load(String gatewayNamespace) {
            return Collections.unmodifiableList(
                VirtualSparkClusterHelper.getVirtualSparkClusterConfigSpecList(gatewayNamespace));
          }
        };
    // TODO: Make 10 seconds configuration
    cache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.SECONDS).build(loader);
  }

  public static SparkClusterCrdDiscovery getInstance() {
    return instance;
  }

  public List<VirtualSparkClusterSpec> getClusters(String gatewayNamespace) {
    return cache.getUnchecked(gatewayNamespace);
  }
}
