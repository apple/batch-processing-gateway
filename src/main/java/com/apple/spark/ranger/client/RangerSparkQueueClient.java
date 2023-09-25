package com.apple.spark.ranger.client;

import static com.apple.spark.core.Constants.QUEUE_LABEL;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

public class RangerSparkQueueClient {
  private static volatile RangerBasePlugin plugin;
  private static LoadingCache<String, Set<String>> userRolesCache;

  private static final String serviceType = "spark-queue";
  private static final String appId = "spark-queue";
  private static final long cacheDuration = 3600000L;
  private static final int cacheMaxCount = 1000;

  private static RangerDefaultAuditHandler auditHandler;

  static {
    plugin = new RangerBasePlugin(serviceType, appId);

    auditHandler = new RangerDefaultAuditHandler();

    plugin.setResultProcessor(auditHandler);

    plugin.init();

    // This is to cache user roles get from ranger server
    userRolesCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheDuration, TimeUnit.MILLISECONDS)
            .maximumSize(cacheMaxCount)
            .build(
                new CacheLoader<String, Set<String>>() {
                  public Set<String> load(String user) throws Exception {
                    Set<String> userRoles = Set.copyOf(plugin.getUserRoles(user, auditHandler));
                    return userRoles;
                  }
                });
  }

  /**
   * Get whether a user is authorized based on queue, access type and user roles from ranger
   *
   * @param queue name
   * @param accessType
   * @param user
   * @return whether a user is authorized
   */
  public static boolean authorize(String queue, String accessType, String user) throws Exception {
    RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
    resource.setValue(QUEUE_LABEL, queue);

    Set<String> userRoles = userRolesCache.get(user);

    RangerAccessRequest request =
        new RangerAccessRequestImpl(resource, accessType, user, null, userRoles);

    RangerAccessResult result = plugin.isAccessAllowed(request, auditHandler);

    return result != null && result.getIsAllowed();
  }
}
