/*
 *
 * This source file is part of the Batch Processing Gateway open source project
 *
 * Copyright 2022 Apple Inc. and the Batch Processing Gateway project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.spark;

import static com.apple.spark.operator.SparkPodSpec.convert2ContainersFromGWInitContainer;

import com.apple.spark.core.ConfigValue;
import com.apple.spark.crd.VirtualSparkClusterSpec;
import com.apple.spark.operator.*;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.core.Configuration;
import io.fabric8.kubernetes.api.model.Container;
import jakarta.validation.Valid;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AppConfig extends Configuration {

  private String applicationContextPath;
  private Map<String, String> defaultSparkConf;

  private List<VirtualSparkClusterSpec> sparkClusters;

  private List<SparkImage> sparkImages;

  private List<ShuffleS3Bucket> shuffleS3Buckets;

  private Ranger ranger;

  private List<Container> driverInitContainers;

  private List<Container> executorInitContainers;

  private String jobDependenciesInitDockerImage;
  private String s3Bucket;
  private String s3Folder;
  private String sparkLogS3Bucket;
  private String sparkLogIndex;
  private String gatewayNamespace;
  private final int batchFileLimit = 2016;
  private String sparkHistoryDns;
  private String gatewayDns;
  private String eksCluster;
  private String sparkHistoryUrl;
  private List<String> allowedUsers;
  private List<String> blockedUsers;
  private List<String> adminUsers;

  private List<QueueConfig> queues;

  private Map<String, QueueConfig> queueConfigs;

  private QueueTokenConfig queueTokenSOPS;

  private DBStorage dbStorageSOPS;

  private Double memoryMbSecondCost;
  private Double vCoreSecondCost;

  private Long statusCacheExpireMillis;

  // Making this a list in the future, considering we may use the same gateway for more than 1
  // accounts
  private String appMetricsDashboardUrl;
  private String splunkBaseUrl;

  public AppConfig() {}

  public String getApplicationContextPath() {
    return applicationContextPath;
  }

  public void setApplicationContextPath(String applicationContextPath) {
    this.applicationContextPath = applicationContextPath;
  }

  public Map<String, String> getDefaultSparkConf() {
    return defaultSparkConf;
  }

  public void setDefaultSparkConf(Map<String, String> defaultSparkConf) {
    this.defaultSparkConf = defaultSparkConf;
  }

  public List<VirtualSparkClusterSpec> getSparkClusters() {
    return sparkClusters;
  }

  public void setSparkClusters(List<VirtualSparkClusterSpec> sparkClusters) {
    this.sparkClusters = sparkClusters;
  }

  public List<SparkImage> getSparkImages() {
    return sparkImages;
  }

  public void setSparkImages(List<SparkImage> sparkImages) {
    this.sparkImages = sparkImages;
  }

  public List<ShuffleS3Bucket> getShuffleS3Buckets() {
    return shuffleS3Buckets;
  }

  public void setShuffleS3Buckets(List<ShuffleS3Bucket> shuffleS3Buckets) {
    this.shuffleS3Buckets = shuffleS3Buckets;
  }

  public String getS3Bucket() {
    return s3Bucket;
  }

  public void setS3Bucket(String s3Bucket) {
    this.s3Bucket = s3Bucket;
  }

  public String getSparkLogS3Bucket() {
    return sparkLogS3Bucket;
  }

  public void setSparkLogS3Bucket(String sparkLogS3Bucket) {
    this.sparkLogS3Bucket = sparkLogS3Bucket;
  }

  public String getSparkLogIndex() {
    return sparkLogIndex;
  }

  public void setSparkLogIndex(String sparkLogIndex) {
    this.sparkLogIndex = sparkLogIndex;
  }

  public int getBatchFileLimit() {
    return batchFileLimit;
  }

  public String getSparkHistoryUrl() {
    return sparkHistoryUrl;
  }

  public String getSparkHistoryDns() {
    return sparkHistoryDns;
  }

  public String getS3Folder() {
    return s3Folder;
  }

  public void setS3Folder(String s3Folder) {
    this.s3Folder = s3Folder;
  }

  public List<String> getAllowedUsers() {
    return allowedUsers;
  }

  public void setAllowedUsers(List<String> allowedUsers) {
    this.allowedUsers = allowedUsers;
  }

  public List<String> getBlockedUsers() {
    return blockedUsers;
  }

  public void setBlockedUsers(List<String> blockedUsers) {
    this.blockedUsers = blockedUsers;
  }

  public List<String> getAdminUsers() {
    return adminUsers;
  }

  public void setAdminUsers(List<String> adminUsers) {
    this.adminUsers = adminUsers;
  }

  public List<QueueConfig> getQueues() {
    return queues;
  }

  public void setQueues(List<QueueConfig> queues) {
    this.queues = queues;
  }

  public Map<String, QueueConfig> getQueueConfigs() {
    if (queueConfigs != null) {
      return queueConfigs;
    }
    if (queues == null) {
      return null;
    }
    queueConfigs = new HashMap<>();
    for (QueueConfig queue : queues) {
      queueConfigs.put(queue.getName(), queue);
    }
    return queueConfigs;
  }

  public Ranger getRanger() {
    return ranger;
  }

  public void setRanger(Ranger ranger) {
    this.ranger = ranger;
  }

  public QueueTokenConfig getQueueTokenSOPS() {
    return queueTokenSOPS;
  }

  public void setQueueTokenSOPS(QueueTokenConfig queueTokenSOPS) {
    this.queueTokenSOPS = queueTokenSOPS;
  }

  public DBStorage getDbStorageSOPS() {
    return dbStorageSOPS;
  }

  public void setDbStorageSOPS(DBStorage dbStorageSOPS) {
    this.dbStorageSOPS = dbStorageSOPS;
  }

  public Long getStatusCacheExpireMillis() {
    return statusCacheExpireMillis;
  }

  public void setStatusCacheExpireMillis(Long statusCacheExpireMillis) {
    this.statusCacheExpireMillis = statusCacheExpireMillis;
  }

  public String getGatewayNamespace() {
    return gatewayNamespace;
  }

  public void setGatewayNamespace(String namespace) {
    this.gatewayNamespace = namespace;
  }

  public Optional<SparkImage> resolveImage(String type, String version) {
    if (sparkImages == null || sparkImages.isEmpty()) {
      return Optional.empty();
    }
    return sparkImages.stream()
        .filter(
            t ->
                typeMatch(t.types, type)
                    && t.version != null
                    && t.version.equalsIgnoreCase(version))
        .findFirst();
  }

  private boolean typeMatch(List<String> types, String type) {
    return types != null && types.stream().anyMatch(t -> t != null && t.equalsIgnoreCase(type));
  }

  public String getEksCluster() {
    return eksCluster;
  }

  public void setEksCluster(String eksCluster) {
    this.eksCluster = eksCluster;
  }

  public String getGatewayDns() {
    return gatewayDns;
  }

  public void setGatewayDns(String gatewayDns) {
    this.gatewayDns = gatewayDns;
  }

  public Double getMemoryMbSecondCost() {
    return memoryMbSecondCost;
  }

  public Double getvCoreSecondCost() {
    return vCoreSecondCost;
  }

  public List<Container> getDriverInitContainers() {
    return driverInitContainers;
  }

  public void setDriverInitContainers(List<InitContainer> driverInitContainers) {

    this.driverInitContainers = convert2ContainersFromGWInitContainer(driverInitContainers);
  }

  public List<Container> getExecutorInitContainers() {
    return executorInitContainers;
  }

  public void setExecutorInitContainers(List<InitContainer> executorInitContainers) {
    this.executorInitContainers = convert2ContainersFromGWInitContainer(executorInitContainers);
  }

  public String getJobDependenciesInitDockerImage() {
    return jobDependenciesInitDockerImage;
  }

  public void setJobDependenciesInitDockerImage(String jobDependenciesInitDockerImage) {
    this.jobDependenciesInitDockerImage = jobDependenciesInitDockerImage;
  }

  public void setAppMetricsDashboardUrl(String appMetricsDashboardUrl) {
    this.appMetricsDashboardUrl = appMetricsDashboardUrl;
  }

  public String getAppMetricsDashboardUrl() {
    return appMetricsDashboardUrl;
  }

  public void setSplunkBaseUrl(String splunkBaseUrl) {
    this.splunkBaseUrl = splunkBaseUrl;
  }

  public String getSplunkBaseUrl() {
    return splunkBaseUrl;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SparkCluster {

    private int weight;
    private String id;
    private String eksCluster;
    private String masterUrl;
    private String caCertDataSOPS;
    private String userName;
    private String userTokenSOPS;
    private Long timeoutMillis;
    private String httpProxy;
    private String httpsProxy;

    private String sparkApplicationNamespace;
    private String sparkServiceAccount;
    private String batchScheduler;
    private List<String> sparkVersions;
    private List<String> queues;
    private Long ttlSeconds;
    private String sparkUIUrl;
    private Map<String, String> sparkConf;
    private SparkUIConfiguration sparkUIOptions;

    private List<Volume> volumes;
    private List<VolumeMount> volumeMounts;
    private DriverSpec driver;
    private ExecutorSpec executor;

    public int getWeight() {
      return weight;
    }

    public void setWeight(int weight) {
      this.weight = weight;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getBatchScheduler() {
      return batchScheduler;
    }

    public void setBatchScheduler(String batchScheduler) {
      this.batchScheduler = batchScheduler;
    }

    public String getMasterUrl() {
      return masterUrl;
    }

    public void setMasterUrl(String masterUrl) {
      this.masterUrl = masterUrl;
    }

    public String getCaCertDataSOPS() {
      return caCertDataSOPS;
    }

    public void setCaCertDataSOPS(String caCertDataSOPS) {
      this.caCertDataSOPS = caCertDataSOPS;
    }

    public String getUserName() {
      return userName;
    }

    public void setUserName(String userName) {
      this.userName = userName;
    }

    public String getUserTokenSOPS() {
      return userTokenSOPS;
    }

    public void setUserTokenSOPS(String userTokenSOPS) {
      this.userTokenSOPS = userTokenSOPS;
    }

    public Long getTimeoutMillis() {
      return timeoutMillis;
    }

    public void setTimeoutMillis(Long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
    }

    public String getHttpProxy() {
      return httpProxy;
    }

    public void setHttpProxy(String httpProxy) {
      this.httpProxy = httpProxy;
    }

    public String getHttpsProxy() {
      return httpsProxy;
    }

    public void setHttpsProxy(String httpsProxy) {
      this.httpsProxy = httpsProxy;
    }

    public String getSparkApplicationNamespace() {
      return sparkApplicationNamespace;
    }

    public void setSparkApplicationNamespace(String sparkApplicationNamespace) {
      this.sparkApplicationNamespace = sparkApplicationNamespace;
    }

    public String getSparkServiceAccount() {
      return sparkServiceAccount;
    }

    public void setSparkServiceAccount(String sparkServiceAccount) {
      this.sparkServiceAccount = sparkServiceAccount;
    }

    public List<String> getSparkVersions() {
      return sparkVersions;
    }

    public void setSparkVersions(List<String> sparkVersions) {
      this.sparkVersions = sparkVersions;
    }

    public List<String> getQueues() {
      return queues;
    }

    public void setQueues(List<String> queues) {
      this.queues = queues;
    }

    public Long getTtlSeconds() {
      return ttlSeconds;
    }

    public void setTtlSeconds(Long ttlSeconds) {
      this.ttlSeconds = ttlSeconds;
    }

    public String getSparkUIUrl() {
      return sparkUIUrl;
    }

    public void setSparkUIUrl(String sparkUIUrl) {
      this.sparkUIUrl = sparkUIUrl;
    }

    public Map<String, String> getSparkConf() {
      return sparkConf;
    }

    public void setSparkConf(Map<String, String> sparkConf) {
      this.sparkConf = sparkConf;
    }

    public SparkUIConfiguration getSparkUIOptions() {
      return sparkUIOptions;
    }

    public void setSparkUIOptions(SparkUIConfiguration sparkUIOptions) {
      this.sparkUIOptions = sparkUIOptions;
    }

    public List<Volume> getVolumes() {
      return volumes;
    }

    public void setVolumes(List<Volume> volumes) {
      this.volumes = volumes;
    }

    public DriverSpec getDriver() {
      return driver;
    }

    public void setDriver(DriverSpec driver) {
      this.driver = driver;
    }

    public ExecutorSpec getExecutor() {
      return executor;
    }

    public void setExecutor(ExecutorSpec executor) {
      this.executor = executor;
    }

    public boolean matchSparkVersion(String sparkVersion) {
      return getSparkVersions() != null
          && getSparkVersions().stream().anyMatch(v -> v.equalsIgnoreCase(sparkVersion));
    }

    public boolean matchQueue(String queue) {
      return getQueues() != null && getQueues().stream().anyMatch(v -> v.equalsIgnoreCase(queue));
    }

    public String getEksCluster() {
      return eksCluster;
    }

    public void setEksCluster(String eksCluster) {
      this.eksCluster = eksCluster;
    }

    public List<VolumeMount> getVolumeMounts() {
      return volumeMounts;
    }

    public void setVolumeMounts(List<VolumeMount> volumeMounts) {
      this.volumeMounts = volumeMounts;
    }

    public String getCaCertDataSOPSDecoded() {
      return ConfigValue.tryGetEncodedSecretValue(caCertDataSOPS);
    }

    public String getUserTokenSOPSDecoded() {
      return ConfigValue.tryGetEncodedSecretValue(userTokenSOPS);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ShuffleS3Bucket {

    private String bucketName;
    private double weight;

    public ShuffleS3Bucket() {}

    public ShuffleS3Bucket(String bucketName, double weight) {
      this.bucketName = bucketName;
      this.weight = weight;
    }

    public double getWeight() {
      return weight;
    }

    public void setWeight(double weight) {
      this.weight = weight;
    }

    public String getBucketName() {
      return bucketName;
    }

    public void setBucketName(String bucketName) {
      this.bucketName = bucketName;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SparkImage {

    private String name;
    private List<String> types;
    private String version;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public List<String> getTypes() {
      return types;
    }

    public void setTypes(List<String> types) {
      this.types = types;
    }

    public String getVersion() {
      return version;
    }

    public void setVersion(String version) {
      this.version = version;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class QueueConfig {

    private String name;
    private Boolean secure;

    private Boolean authorizeEnabled = false;

    /**
     * 11/10/2023 - We need an additional Notary enabled flag This allows us to roll out
     * Narrative/Notary setup per-queue*
     */
    private Boolean notaryEnabled = false;

    private Boolean ircEnabled = false;

    private List<String> users;
    private Long maxRunningMillis;
    private String driverNodeLabelKey;
    private String executorNodeLabelKey;
    private String executorSpotNodeLabelKey;
    private List<String> driverNodeLabelValues;
    private List<String> executorNodeLabelValues;
    private List<String> executorSpotNodeLabelValues;
    private double driverCPUBufferRatio = 1.0;
    private double executorCPUBufferRatio = 1.0;
    private double driverMemBufferRatio = 1.0;
    private double executorMemBufferRatio = 1.0;
    private List<String> allowedAssumeRoles;
    private String defaultAssumeRole;
    private String defaultQueueArch;
    private boolean disableRuntimeLimit;
    private boolean supportGpu = false;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Boolean getSecure() {
      return secure;
    }

    public void setSecure(Boolean secure) {
      this.secure = secure;
    }

    public Boolean getAuthorizeEnabled() {
      return authorizeEnabled;
    }

    // Getter for Notary Enabled
    public Boolean getNotaryEnabled() {
      return notaryEnabled;
    }

    public void setAuthorizeEnabled(Boolean authorizeEnabled) {
      this.authorizeEnabled = authorizeEnabled;
    }

    // Setter for Notary Enabled
    public void setNotaryEnabled(Boolean notaryEnabled) {
      this.notaryEnabled = notaryEnabled;
    }

    public Boolean getIrcEnabled() {
      return ircEnabled;
    }

    public void setIrcEnabled(Boolean ircEnabled) {
      this.ircEnabled = ircEnabled;
    }

    public List<String> getUsers() {
      return users;
    }

    public void setUsers(List<String> users) {
      this.users = users;
    }

    public Long getMaxRunningMillis() {
      return maxRunningMillis;
    }

    public void setMaxRunningMillis(Long maxRunningMillis) {
      this.maxRunningMillis = maxRunningMillis;
    }

    public boolean containUser(String user) {
      return users != null && users.stream().anyMatch(u -> u.equalsIgnoreCase(user));
    }

    public String getDriverNodeLabelKey() {
      return driverNodeLabelKey;
    }

    public void setDriverNodeLabelKey(String driverNodeLabelKey) {
      this.driverNodeLabelKey = driverNodeLabelKey;
    }

    public String getExecutorNodeLabelKey() {
      return executorNodeLabelKey;
    }

    public void setExecutorNodeLabelKey(String executorNodeLabelKey) {
      this.executorNodeLabelKey = executorNodeLabelKey;
    }

    public List<String> getDriverNodeLabelValues() {
      return driverNodeLabelValues;
    }

    public void setDriverNodeLabelValues(List<String> driverNodeLabelValues) {
      this.driverNodeLabelValues = driverNodeLabelValues;
    }

    public List<String> getExecutorNodeLabelValues() {
      return executorNodeLabelValues;
    }

    public void setExecutorNodeLabelValues(List<String> executorNodeLabelValues) {
      this.executorNodeLabelValues = executorNodeLabelValues;
    }

    public List<String> getExecutorSpotNodeLabelValues() {
      return executorSpotNodeLabelValues;
    }

    public void setExecutorSpotNodeLabelValues(List<String> executorSpotNodeLabelValues) {
      this.executorSpotNodeLabelValues = executorSpotNodeLabelValues;
    }

    public Double getDriverCPUBufferRatio() {
      return driverCPUBufferRatio;
    }

    public void setDriverCPUBufferRatio(String driverCPUBufferRatio) {
      this.driverCPUBufferRatio = Double.parseDouble(driverCPUBufferRatio);
    }

    public Double getExecutorCPUBufferRatio() {
      return executorCPUBufferRatio;
    }

    public void setExecutorCPUBufferRatio(String executorCPUBufferRatio) {
      this.executorCPUBufferRatio = Double.parseDouble(executorCPUBufferRatio);
    }

    public Double getDriverMemBufferRatio() {
      return driverMemBufferRatio;
    }

    public void setDriverMemBufferRatio(String driverMemBufferRatio) {
      this.driverMemBufferRatio = Double.parseDouble(driverMemBufferRatio);
    }

    public Double getExecutorMemBufferRatio() {
      return executorMemBufferRatio;
    }

    public void setExecutorMemBufferRatio(String executorMemBufferRatio) {
      this.executorMemBufferRatio = Double.parseDouble(executorMemBufferRatio);
    }

    public List<String> getAllowedAssumeRoles() {
      return allowedAssumeRoles;
    }

    public void setAllowedAssumeRoles(List<String> allowedAssumeRoles) {
      this.allowedAssumeRoles = allowedAssumeRoles;
    }

    public boolean allowAssumeRole(String roleArn) {
      return allowedAssumeRoles != null
          && allowedAssumeRoles.stream().anyMatch(t -> t.equalsIgnoreCase(roleArn));
    }

    public String getDefaultAssumeRole() {
      return defaultAssumeRole;
    }

    public void setDefaultAssumeRole(String defaultAssumeRole) {
      this.defaultAssumeRole = defaultAssumeRole;
    }

    public String getDefaultQueueArch() {
      return defaultQueueArch;
    }

    public void setDefaultQueueArch(String defaultQueueArch) {
      this.defaultQueueArch = defaultQueueArch;
    }

    public Boolean getSupportGpu() {
      return supportGpu;
    }

    public void setSupportGpu(Boolean supportGpu) {
      this.supportGpu = supportGpu;
    }

    public String getExecutorSpotNodeLabelKey() {
      return executorSpotNodeLabelKey;
    }

    public void setExecutorSpotNodeLabelKey(String executorSpotNodeLabelKey) {
      this.executorSpotNodeLabelKey = executorSpotNodeLabelKey;
    }

    public boolean isDisableRuntimeLimit() {
      return disableRuntimeLimit;
    }

    public void setDisableRuntimeLimit(boolean disableRuntimeLimit) {
      this.disableRuntimeLimit = disableRuntimeLimit;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class QueueTokenConfig {

    private List<String> secrets;

    public List<String> getSecrets() {
      return secrets;
    }

    public void setSecrets(List<String> secrets) {
      this.secrets = secrets;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Ranger {
    private String sparkQueuePolicyRestUrl;
    private String sparkQueueXasecureAuditDestinationSolrUrls;

    private Long userGroupsCacheDurationInMillis;

    public Long getUserGroupsCacheDurationInMillis() {
      return userGroupsCacheDurationInMillis;
    }

    public void setUserGroupsCacheDurationInMillis(Long userGroupsCacheDurationInMillis) {
      this.userGroupsCacheDurationInMillis = userGroupsCacheDurationInMillis;
    }

    public String getSparkQueuePolicyRestUrl() {
      return sparkQueuePolicyRestUrl;
    }

    public void setSparkQueuePolicyRestUrl(String sparkQueuePolicyRestUrl) {
      this.sparkQueuePolicyRestUrl = sparkQueuePolicyRestUrl;
    }

    public String getSparkQueueXasecureAuditDestinationSolrUrls() {
      return sparkQueueXasecureAuditDestinationSolrUrls;
    }

    public void setSparkQueueXasecureAuditDestinationSolrUrls(
        String sparkQueueXasecureAuditDestinationSolrUrls) {
      this.sparkQueueXasecureAuditDestinationSolrUrls = sparkQueueXasecureAuditDestinationSolrUrls;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DBStorage {

    private String connectionString;
    private String user;
    private String password;
    private String dbName;

    public String getConnectionString() {
      return connectionString;
    }

    public void setConnectionString(String connectionString) {
      this.connectionString = connectionString;
    }

    public String getUser() {
      return user;
    }

    public void setUser(String user) {
      this.user = user;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public String getDbName() {
      return dbName;
    }

    public void setDbName(String dbName) {
      this.dbName = dbName;
    }

    public String getPasswordDecodedValue() {
      return ConfigValue.tryGetEncodedSecretValue(password);
    }
  }

  /** Apple Internal Notary related configs. */
  @Valid private NotaryAppConfig notary;

  public NotaryAppConfig getNotary() {
    return notary;
  }

  public void setNotary(NotaryAppConfig notary) {
    this.notary = notary;
  }

  public static class NotaryAppConfig {
    @JsonProperty private String appNameSpace;
    @JsonProperty private String turiDirectoryApiKey;
    @JsonProperty private List<Long> proxyAllowedGroupIDList;
    @JsonProperty private String narrativeTuriPodDomain;
    @JsonProperty private Integer aacRateLimit;

    public Integer getAacRateLimit() {
      return aacRateLimit;
    }

    public void setAacRateLimit(Integer aacRateLimit) {
      this.aacRateLimit = aacRateLimit;
    }

    public String getNarrativeTuriPodDomain() {
      return narrativeTuriPodDomain;
    }

    public void setNarrativeTuriPodDomain(String narrativeTuriPodDomain) {
      this.narrativeTuriPodDomain = narrativeTuriPodDomain;
    }

    public String getAppNameSpace() {
      return appNameSpace;
    }

    public void setAppNameSpace(String appNameSpace) {
      this.appNameSpace = appNameSpace;
    }

    public String getTuriDirectoryApiKey() {
      return turiDirectoryApiKey;
    }

    public void setTuriDirectoryApiKey(String turiDirectoryApiKey) {
      this.turiDirectoryApiKey = turiDirectoryApiKey;
    }

    public List<Long> getProxyAllowedGroupIDList() {
      return proxyAllowedGroupIDList;
    }

    public void setProxyAllowedGroupIDList(List<Long> proxyAllowedGroupIDList) {
      this.proxyAllowedGroupIDList = proxyAllowedGroupIDList;
    }
  }

  /** Apple Internal Conductor related configs. */
  @Valid private ConductorConfig conductor;

  public ConductorConfig getConductor() {
    return conductor;
  }

  public void setConductor(ConductorConfig conductor) {
    this.conductor = conductor;
  }

  public static class ConductorConfig {
    @JsonProperty private String conductorEndpoint;

    public String getConductorEndpoint() {
      return conductorEndpoint;
    }

    public void setConductorEndpoint(String conductorEndpoint) {
      this.conductorEndpoint = conductorEndpoint;
    }
  }

  @Valid private IRCConfig irc;

  public IRCConfig getIrc() {
    return irc;
  }

  public void setIrc(IRCConfig irc) {
    this.irc = irc;
  }

  public static class IRCConfig {
    @JsonProperty private String ircEndpoint;
    @JsonProperty private Boolean ircSecurityEnabled;

    public String getIrcEndpoint() {
      return ircEndpoint;
    }

    public void setIrcEndpoint(String ircEndpoint) {
      this.ircEndpoint = ircEndpoint;
    }

    public Boolean getIrcSecurityEnabled() {
      return ircSecurityEnabled;
    }

    public void setIrcSecurityEnabled(Boolean ircSecurityEnabled) {
      this.ircSecurityEnabled = ircSecurityEnabled;
    }
  }
}
