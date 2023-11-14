package com.apple.spark.ranger.client;

import static com.apple.spark.core.Constants.QUEUE_LABEL;

import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerSparkQueueClient {
  private static final Logger logger = LoggerFactory.getLogger(RangerSparkQueueClient.class);
  private static volatile RangerBasePlugin plugin;
  private static final String serviceType = "spark-queue";
  private static final String appId = "skate-spark-queue-authorizer";

  private static RangerDefaultAuditHandler auditHandler;

  private final ConcurrentMap<String, Set<String>> userGroups = new ConcurrentHashMap<>();

  private Timer timer = new Timer(true);

  private Long lastKnownUserStoreVersion = Long.valueOf(-1);
  private Long lastActivationTime = System.currentTimeMillis();

  private static final Long DEFAULT_MIN_CACHE_REFRESH_MS = Long.valueOf(300000);

  public RangerSparkQueueClient(
      String policyRestUrl, String auditSolrUrl, Long userGroupsCacheDurationInMillis) {
    RangerPluginConfig rangerPluginConfig = getRangerPluginConfig(policyRestUrl, auditSolrUrl);
    plugin = new RangerBasePlugin(rangerPluginConfig);

    auditHandler = new RangerDefaultAuditHandler();

    plugin.setResultProcessor(auditHandler);

    plugin.init();

    populateUserGroupsCache();
    long timerDelayPeriod =
        userGroupsCacheDurationInMillis != null
            ? Math.max(userGroupsCacheDurationInMillis, DEFAULT_MIN_CACHE_REFRESH_MS)
            : DEFAULT_MIN_CACHE_REFRESH_MS;
    if (userGroupsCacheDurationInMillis != null
        && userGroupsCacheDurationInMillis < DEFAULT_MIN_CACHE_REFRESH_MS) {
      logger.warn(
          String.format(
              "Config userGroupsCacheDurationInMillis with value %s is not used since it is smaller than required minimum value %s",
              userGroupsCacheDurationInMillis, DEFAULT_MIN_CACHE_REFRESH_MS));
    }
    timer.scheduleAtFixedRate(
        new TimerTask() {
          @Override
          public void run() {
            logger.info("Running task to get user store if updated, from Ranger Admin server");
            populateUserGroupsCache();
          }
        },
        timerDelayPeriod,
        timerDelayPeriod);
  }

  private static RangerPluginConfig getRangerPluginConfig(
      String policyRestUrl, String auditSolrUrl) {
    RangerPluginConfig config =
        new RangerPluginConfig(serviceType, serviceType, appId, null, null, null);

    // audit config
    if (auditSolrUrl != null) {
      config.set("xasecure.audit.is.enabled", "true");
      config.set("xasecure.audit.destination.solr", "true");
      config.set("xasecure.audit.destination.solr.urls", auditSolrUrl);
      config.set("xasecure.audit.destination.solr.batch.batch.interval.ms", "5000");
      config.set("xasecure.audit.provider.solr.summary.enabled", "true");
    } else {
      logger.warn("ranger audit is not enabled.");
    }

    // security config
    config.set(String.format("ranger.plugin.%s.service.name", serviceType), serviceType);
    config.set(String.format("ranger.plugin.%s.policy.rest.url", serviceType), policyRestUrl);
    config.set(String.format("ranger.plugin.%s.policy.pollIntervalMs", serviceType), "300000");
    config.set(
        String.format("ranger.plugin.%s.policy.rest.client.connection.timeoutMs", serviceType),
        "30000");
    config.set(
        String.format("ranger.plugin.%s.policy.rest.client.read.timeoutMs", serviceType), "30000");

    return config;
  }

  /**
   * Get whether a user is authorized based on queue, access type and user roles from ranger
   *
   * @param queueAccessTypeAndUser QueueAccessTypeAndUser
   * @return whether a user is authorized
   */
  public boolean authorize(QueueAccessTypeAndUser queueAccessTypeAndUser) throws Exception {
    RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
    resource.setValue(QUEUE_LABEL, queueAccessTypeAndUser.getQueue());

    String user = queueAccessTypeAndUser.getUser();

    RangerAccessRequest request =
        new RangerAccessRequestImpl(
            resource, queueAccessTypeAndUser.getAccessType(), user, userGroups.get(user), null);

    RangerAccessResult result = plugin.isAccessAllowed(request, auditHandler);

    return result != null && result.getIsAllowed();
  }

  private void populateUserGroupsCache() {
    try {
      RangerUserStore userStore =
          plugin
              .getPluginContext()
              .getAdminClient()
              .getUserStoreIfUpdated(lastKnownUserStoreVersion, lastActivationTime);
      if (userStore != null) {
        lastKnownUserStoreVersion = userStore.getUserStoreVersion();
        lastActivationTime = System.currentTimeMillis();
        logger.info(
            "User group mapping lastKnownVersion: "
                + lastKnownUserStoreVersion
                + " updateTime: "
                + userStore.getUserStoreUpdateTime());
        if (userStore.getUserGroupMapping() != null) {
          logger.info("Size of user group mapping is " + userStore.getUserGroupMapping().size());
          logger.debug("User store returned is \n" + userStore.getUserGroupMapping());
          // first remove the existing users from cache that are not present in latest user store
          for (String existingUser : userGroups.keySet()) {
            if (!userStore.getUserGroupMapping().containsKey(existingUser)) {
              userGroups.remove(existingUser);
            }
          }
          // then add any new user's groups or update existing user's groups if they are not the
          // same in cache
          for (Map.Entry<String, Set<String>> entry : userStore.getUserGroupMapping().entrySet()) {
            if (!userGroups.containsKey(entry.getKey())
                || !userGroups.get(entry.getKey()).equals(entry.getValue())) {
              userGroups.put(entry.getKey(), entry.getValue());
            }
          }
        } else {
          // Ranger Admin server returned a null user group mapping so clear the cache
          userGroups.clear();
        }
      } else {
        logger.info(
            "Returned user store is null. It is very likely because userStore has not changed");
      }
    } catch (Exception e) {
      logger.warn(
          "Exception thrown while getting user store from Ranger server. Ignoring exception to work"
              + "with last known version "
              + lastKnownUserStoreVersion);
    }
  }
}
