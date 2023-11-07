package com.apple.spark.ranger.client;

import static com.apple.spark.core.Constants.QUEUE_LABEL;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerSparkQueueClient {
  private static final Logger logger = LoggerFactory.getLogger(RangerSparkQueueClient.class);
  private static volatile RangerBasePlugin plugin;
  private static LoadingCache<QueueAccessTypeAndUser, Boolean> queueAccessCache;
  private static LoadingCache<String, Set<String>> userRolesCache;

  private static final String serviceType = "spark-queue";
  private static final String appId = "spark-queue";
  private static final long cacheDuration = 3600000L;
  private static final int cacheMaxCount = 1000;

  private static RangerDefaultAuditHandler auditHandler;

  public RangerSparkQueueClient(
      String policyRestUrl, String auditSolrUrl, String user, String password) {
    RangerPluginConfig rangerPluginConfig =
        getRangerPluginConfig(policyRestUrl, auditSolrUrl, user, password);

    this.plugin = new RangerBasePlugin(rangerPluginConfig);

    auditHandler = new RangerDefaultAuditHandler();

    plugin.setResultProcessor(auditHandler);

    plugin.init();

    // This is to cache user roles get from ranger server
    userRolesCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheDuration, TimeUnit.MILLISECONDS)
            .maximumSize(cacheMaxCount)
            .build(
                new CacheLoader<>() {
                  public Set<String> load(String user) throws Exception {
                    Set<String> userRoles = Set.copyOf(plugin.getUserRoles(user, auditHandler));
                    return userRoles;
                  }
                });

    // This is to cache authorize result from ranger server
    queueAccessCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheDuration, TimeUnit.MILLISECONDS)
            .maximumSize(cacheMaxCount)
            .build(
                new CacheLoader<>() {
                  public Boolean load(QueueAccessTypeAndUser queueAccessTypeAndUser)
                      throws Exception {
                    return authorizeAndUpdateCache(queueAccessTypeAndUser);
                  }
                });
  }

  private static RangerPluginConfig getRangerPluginConfig(
      String policyRestUrl, String auditSolrUrl, String user, String password) {
    RangerPluginConfig config = new RangerPluginConfig(serviceType, appId, appId, null, null, null);

    // audit config
    if (auditSolrUrl != null) {
      config.set("xasecure.audit.is.enabled", "true");
      config.set("xasecure.audit.destination.solr", "true");
      config.set("xasecure.audit.destination.solr.urls", auditSolrUrl);
      config.set("xasecure.audit.destination.solr.batch.batch.interval.ms", "5000");
    } else {
      logger.warn("ranger audit is not enavled.");
    }

    // security config
    config.set(
        String.format("ranger.plugin.%s.policy.source.impl", serviceType),
        "com.apple.spark.ranger.client.AdminRESTClient");
    config.set(String.format("ranger.plugin.%s.service.name", serviceType), appId);
    config.set(String.format("ranger.plugin.%s.policy.rest.url", serviceType), policyRestUrl);
    config.set(String.format("ranger.plugin.%s.policy.pollIntervalMs", serviceType), "30000");
    config.set(
        String.format("ranger.plugin.%s.policy.rest.client.connection.timeoutMs", serviceType),
        "30000");
    config.set(
        String.format("ranger.plugin.%s.policy.rest.client.read.timeoutMs", serviceType), "30000");

    config.set(String.format("ranger.plugin.%s.plugin.user", serviceType), user);
    config.set(String.format("ranger.plugin.%s.plugin.password", serviceType), password);

    return config;
  }

  /**
   * Get whether a user is authorized based on queue, access type and user roles from ranger
   *
   * @param queueAccessTypeAndUser QueueAccessTypeAndUser
   * @return whether a user is authorized
   */
  private static boolean authorizeAndUpdateCache(QueueAccessTypeAndUser queueAccessTypeAndUser)
      throws Exception {
    RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
    resource.setValue(QUEUE_LABEL, queueAccessTypeAndUser.getQueue());

    String user = queueAccessTypeAndUser.getUser();

    Set<String> userRoles = userRolesCache.get(user);

    RangerAccessRequest request =
        new RangerAccessRequestImpl(
            resource, queueAccessTypeAndUser.getAccessType(), user, null, userRoles);

    RangerAccessResult result = plugin.isAccessAllowed(request, auditHandler);

    return result != null && result.getIsAllowed();
  }

  public static boolean authorize(QueueAccessTypeAndUser queueAccessTypeAndUser) throws Exception {
    return queueAccessCache.get(queueAccessTypeAndUser);
  }
}
