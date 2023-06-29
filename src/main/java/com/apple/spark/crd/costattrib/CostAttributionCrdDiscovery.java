package com.apple.spark.crd.costattrib;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CostAttributionCrdDiscovery {
  private static final Logger logger = LoggerFactory.getLogger(CostAttributionCrdDiscovery.class);

  private static final CostAttributionCrdDiscovery instance = new CostAttributionCrdDiscovery();

  private static LoadingCache<String, List<CostAttributionSpec>> cache;

  //    tryGetServiceAccountNamespace
  public CostAttributionCrdDiscovery() {
    CacheLoader<String, List<CostAttributionSpec>> loader =
        new CacheLoader<>() {
          @Override
          public List<CostAttributionSpec> load(String gatewayNamespace) {
            logger.info("Loading CostAttribution in namespace {}", gatewayNamespace);
            List<CostAttributionSpec> list =
                Collections.unmodifiableList(
                    CostAttributionHelper.getCostAttributionConfigSpecList(gatewayNamespace));
            String str =
                list.stream()
                    .map(CostAttributionCrdDiscovery::getLogString)
                    .collect(Collectors.joining(", "));
            logger.info("Loaded CostAttribution in namespace {}: {}", gatewayNamespace, str);
            return list;
          }
        };
    // TODO: Make 30 seconds configuration
    cache = CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.SECONDS).build(loader);
  }

  public static CostAttributionCrdDiscovery getInstance() {
    return instance;
  }

  private static String getLogString(CostAttributionSpec spec) {
    if (spec == null) {
      return "null";
    }

    String domainStr = "";
    if (spec.getDomain() != null) {
      domainStr = spec.getDomain();
    }
    String versionStr = "";
    if (spec.getVersion() != null) {
      versionStr = spec.getVersion();
    }
    return String.format("domain: %s, version: %s", domainStr, versionStr);
  }

  public List<CostAttributionSpec> getCostAttribs(String gatewayNamespace) {
    List<CostAttributionSpec> list = new ArrayList<>();
    try {
      list = cache.getUnchecked(gatewayNamespace);
    } catch (Throwable ex) {
      logger.warn("Failed to get CostAttribution from " + gatewayNamespace, ex);
    }
    return list;
  }
}
