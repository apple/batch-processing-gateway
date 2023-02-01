package com.apple.spark.crd;

import com.apple.spark.core.ConfigValue;
import com.apple.spark.operator.*;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

/**
 * This class is created to match Spark Cluster Config API. Please note that this class is a wrapper
 * for a namespace in a k8s cluster. The Spark Cluster in the gateway config is actually referring
 * to one of the namespaces in the remote spark cluster
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VirtualSparkClusterSpec {

  private int weight;
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
