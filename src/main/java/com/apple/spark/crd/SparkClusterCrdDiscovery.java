package com.apple.spark.crd;

import static com.apple.spark.core.Constants.*;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkClusterCrdDiscovery {
  private static final Logger logger = LoggerFactory.getLogger(SparkClusterCrdDiscovery.class);

  private static final SparkClusterCrdDiscovery instance = new SparkClusterCrdDiscovery();

  private static LoadingCache<String, List<VirtualSparkClusterSpec>> cache;

  //    tryGetServiceAccountNamespace
  public SparkClusterCrdDiscovery() {
    CacheLoader<String, List<VirtualSparkClusterSpec>> loader =
        new CacheLoader<>() {
          @Override
          public List<VirtualSparkClusterSpec> load(String gatewayNamespace) {
            logger.info("Loading VirtualSparkCluster in namespace {}", gatewayNamespace);
            List<VirtualSparkClusterSpec> list =
                Collections.unmodifiableList(
                    VirtualSparkClusterHelper.getVirtualSparkClusterConfigSpecList(
                        gatewayNamespace));
            String str = list.stream().map(t -> getLogString(t)).collect(Collectors.joining(", "));
            logger.info("Loaded VirtualSparkCluster in namespace {}: {}", gatewayNamespace, str);
            return list;
          }
        };
    // TODO: Make 30 seconds configuration
    cache = CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.SECONDS).build(loader);
  }

  public static SparkClusterCrdDiscovery getInstance() {
    return instance;
  }

  private static String getLogString(VirtualSparkClusterSpec spec) {
    if (spec == null) {
      return "null";
    }

    String sparkVersionsStr = "";
    if (spec.getSparkVersions() != null) {
      sparkVersionsStr = spec.getSparkVersions().stream().collect(Collectors.joining(", "));
    }
    String queuesStr = "";
    if (spec.getQueues() != null) {
      queuesStr = spec.getQueues().stream().collect(Collectors.joining(", "));
    }
    return String.format(
        "[id: %s, sparkVersions: [%s], queues: [%s]]", spec.getId(), sparkVersionsStr, queuesStr);
  }

  public List<VirtualSparkClusterSpec> getClusters(String gatewayNamespace) {
    List<VirtualSparkClusterSpec> list = new ArrayList<>();
    try {
      list = cache.getUnchecked(gatewayNamespace);
    } catch (Throwable ex) {
      logger.warn("Failed to get VirtualSparkCluster from " + gatewayNamespace, ex);
    }
    return list;
  }
}
