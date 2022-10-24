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

package com.apple.spark.core;

import static com.apple.spark.core.BatchSchedulerConstants.PLACEHOLDER_TIMEOUT_IN_SECONDS;
import static com.apple.spark.core.BatchSchedulerConstants.YUNIKORN_ROOT_QUEUE;
import static com.apple.spark.core.BatchSchedulerConstants.YUNIKORN_SPARK_DEFAULT_QUEUE;
import static com.apple.spark.core.Constants.*;
import static com.apple.spark.core.SparkConstants.CORE_LIMIT_RATIO;
import static com.apple.spark.core.SparkConstants.DRIVER_CPU_BUFFER_RATIO;
import static com.apple.spark.core.SparkConstants.DRIVER_MEM_BUFFER_RATIO;
import static com.apple.spark.core.SparkConstants.EXECUTOR_CPU_BUFFER_RATIO;
import static com.apple.spark.core.SparkConstants.EXECUTOR_MEM_BUFFER_RATIO;

import com.apple.spark.AppConfig;
import com.apple.spark.AppConfig.SparkCluster;
import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.operator.Affinity;
import com.apple.spark.operator.BatchSchedulerConfiguration;
import com.apple.spark.operator.DriverSpec;
import com.apple.spark.operator.ExecutorSpec;
import com.apple.spark.operator.NodeAffinity;
import com.apple.spark.operator.NodeSelectorOperator;
import com.apple.spark.operator.NodeSelectorRequirement;
import com.apple.spark.operator.NodeSelectorTerm;
import com.apple.spark.operator.RequiredDuringSchedulingIgnoredDuringExecutionTerm;
import com.apple.spark.operator.SparkApplicationSpec;
import com.apple.spark.operator.SparkUIConfiguration;
import com.apple.spark.operator.Volume;
import com.apple.spark.util.ExceptionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.fabric8.kubernetes.api.model.PodDNSConfig;
import io.fabric8.kubernetes.api.model.PodDNSConfigOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationSubmissionHelper {

  private static final Logger logger = LoggerFactory.getLogger(ApplicationSubmissionHelper.class);

  private static final String FILE_PATH_REGEX = "([a-zA-Z~])?(\\/[a-zA-Z0-9_.-]+)+\\/?";

  public static SubmitApplicationRequest parseSubmitRequest(
      String requestBody, String contentType) {
    SubmitApplicationRequest request;

    boolean parseRequestAsYaml =
        !StringUtils.isEmpty(contentType) && contentType.toLowerCase().contains("yaml");

    // parse request as YAML or JSON, depending on the content type
    if (parseRequestAsYaml) {
      ObjectMapper yamlObjectMapper =
          new ObjectMapper(
              new YAMLFactory()
                  .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                  .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
      try {
        request = yamlObjectMapper.readValue(requestBody, SubmitApplicationRequest.class);
      } catch (Throwable e) {
        logger.warn("Failed to parse request as YAML", e);
        throw new WebApplicationException(
            String.format("Invalid YAML request: %s", ExceptionUtils.getExceptionNameAndMessage(e)),
            Response.Status.BAD_REQUEST);
      }
    } else {
      // try to parse the request body as JSON
      try {
        request = new ObjectMapper().readValue(requestBody, SubmitApplicationRequest.class);
      } catch (Throwable e) {
        // when users use curl to submit jobs, sometimes they send the file path
        // instead of the file content
        // this can happen when they misuse the @/file/path syntax
        if (looksLikeFilePath(requestBody)) {
          logger.warn("Request body was rejected because it looked like a file path");
          throw new WebApplicationException(
              String.format(
                  "Invalid request body. It actually looks like a file path. If you're using curl,"
                      + " make sure you are passing the file content (not the file path) as the"
                      + " request body."),
              Response.Status.BAD_REQUEST);
        } else {
          logger.warn("Failed to parse request as JSON", e);
          throw new WebApplicationException(
              String.format(
                  "Invalid JSON request: %s", ExceptionUtils.getExceptionNameAndMessage(e)),
              Response.Status.BAD_REQUEST);
        }
      }
    }

    return request;
  }

  /**
   * When a job is submitted via Airflow, the username can be a system account. In that case, return
   * the DAG username as a proxy user. This will help some logging or metrics to have better
   * granularity.
   *
   * @param user the username that can potentially be an Airflow system account
   * @param dagUser the username specified in DAG
   * @return the DAG username if user is a system account
   */
  public static String getProxyUser(String user, String dagUser) {
    if (AIRFLOW_SYSTEM_ACCOUNTS.contains(user)) {
      return dagUser;
    }

    return user;
  }

  /** examine a request body and see if the request body looks like a file path */
  public static boolean looksLikeFilePath(String requestBody) {
    return Pattern.matches(FILE_PATH_REGEX, requestBody);
  }

  public static Map<String, String> getSparkConf(
      String submissionId,
      SubmitApplicationRequest request,
      Map<String, String> defaultSparkConf,
      AppConfig.SparkCluster sparkCluster) {

    Map<String, String> sparkConf = null;

    if (defaultSparkConf != null) {
      if (sparkConf == null) {
        sparkConf = new HashMap<>();
      }
      Map<String, String> filteredDefaultSparkConf = applyFeatureGate(request, defaultSparkConf);

      for (Map.Entry<String, String> entry : filteredDefaultSparkConf.entrySet()) {
        sparkConf.put(entry.getKey(), substitutionSparkConfigValue(entry.getValue(), submissionId));
      }
    }

    if (sparkCluster.getSparkConf() != null) {
      if (sparkConf == null) {
        sparkConf = new HashMap<>();
      }
      for (Map.Entry<String, String> entry : sparkCluster.getSparkConf().entrySet()) {
        sparkConf.put(entry.getKey(), substitutionSparkConfigValue(entry.getValue(), submissionId));
      }
    }

    if (request.getSparkConf() != null) {
      if (sparkConf == null) {
        sparkConf = new HashMap<>();
      }
      for (Map.Entry<String, String> entry : request.getSparkConf().entrySet()) {
        sparkConf.put(entry.getKey(), entry.getValue());
      }
    }

    if (!StringUtils.isEmpty(request.getApplicationName())) {
      if (sparkConf == null) {
        sparkConf = new HashMap<>();
      }
      sparkConf.put(SparkConstants.SPARK_APP_NAME_CONFIG, request.getApplicationName());
    }

    return sparkConf;
  }

  public static SparkUIConfiguration getSparkUIConfiguration(
      String submissionId, AppConfig.SparkCluster sparkCluster) {
    SparkUIConfiguration sparkJobUIConfiguration = null;

    if (sparkCluster.getSparkUIOptions() != null) {
      sparkJobUIConfiguration = new SparkUIConfiguration(sparkCluster.getSparkUIOptions());
      //      sparkSpec.setSparkUIOptions(sparkJobUIConfiguration);

      if (sparkJobUIConfiguration.getIngressAnnotations() != null) {
        Map<String, String> expandedValues = new HashMap<>();
        for (Map.Entry<String, String> entry :
            sparkJobUIConfiguration.getIngressAnnotations().entrySet()) {
          expandedValues.put(
              entry.getKey(),
              entry
                  .getValue()
                  .replace(Constants.SPARK_APPLICATION_RESOURCE_NAME_VAR, submissionId));
        }
        sparkJobUIConfiguration.setIngressAnnotations(expandedValues);
      }
    }

    return sparkJobUIConfiguration;
  }

  public static List<Volume> getVolumes(
      SubmitApplicationRequest request, AppConfig.SparkCluster sparkCluster) {

    // if there are volumes in the request, use that to populate spark spec
    if (request.getVolumes() != null && !request.getVolumes().isEmpty()) {
      return request.getVolumes();
    }

    // if there are volumes in the spark cluster, use that to populate spark spec
    if (sparkCluster.getVolumes() != null && !sparkCluster.getVolumes().isEmpty()) {
      return sparkCluster.getVolumes();
    }

    return null;
  }

  public static void populateEnv(
      SparkApplicationSpec sparkSpec,
      SubmitApplicationRequest request,
      AppConfig.SparkCluster sparkCluster) {
    if (sparkCluster.getDriver() != null && sparkCluster.getDriver().getEnv() != null) {
      // there is env in spark cluster driver configure, copy it to spark spec
      if (sparkSpec.getDriver() == null) {
        sparkSpec.setDriver(new DriverSpec());
      }
      if (sparkSpec.getDriver().getEnv() == null) {
        sparkSpec.getDriver().setEnv(new ArrayList<>());
      }
      SparkSpecHelper.copyEnv(sparkCluster.getDriver().getEnv(), sparkSpec.getDriver().getEnv());
    }
    if (request.getDriver() != null && request.getDriver().getEnv() != null) {
      // there is env in request driver configure, copy it to spark spec
      if (sparkSpec.getDriver() == null) {
        sparkSpec.setDriver(new DriverSpec());
      }
      if (sparkSpec.getDriver().getEnv() == null) {
        sparkSpec.getDriver().setEnv(new ArrayList<>());
      }
      SparkSpecHelper.copyEnv(request.getDriver().getEnv(), sparkSpec.getDriver().getEnv());
    }
    if (sparkCluster.getExecutor() != null && sparkCluster.getExecutor().getEnv() != null) {
      // there is env in spark cluster executor configure, copy it to spark spec
      if (sparkSpec.getExecutor() == null) {
        sparkSpec.setExecutor(new ExecutorSpec());
      }
      if (sparkSpec.getExecutor().getEnv() == null) {
        sparkSpec.getExecutor().setEnv(new ArrayList<>());
      }
      SparkSpecHelper.copyEnv(
          sparkCluster.getExecutor().getEnv(), sparkSpec.getExecutor().getEnv());
    }
    if (request.getExecutor() != null && request.getExecutor().getEnv() != null) {
      // there is env in request executor configure, copy it to spark spec
      if (sparkSpec.getExecutor() == null) {
        sparkSpec.setExecutor(new ExecutorSpec());
      }
      if (sparkSpec.getExecutor().getEnv() == null) {
        sparkSpec.getExecutor().setEnv(new ArrayList<>());
      }
      SparkSpecHelper.copyEnv(request.getExecutor().getEnv(), sparkSpec.getExecutor().getEnv());
    }
  }

  public static String generateSubmissionId(String clusterId) {
    return String.format("%s-%s", clusterId, UUID.randomUUID().toString().replace("-", ""));
  }

  public static String generateSubmissionId(String clusterId, String submissionIdSuffix) {
    if (submissionIdSuffix != null && !submissionIdSuffix.isEmpty()) {
      return String.format("%s-%s", generateSubmissionId(clusterId), submissionIdSuffix);
    }

    return generateSubmissionId(clusterId);
  }

  public static String getClusterIdFromSubmissionId(String submissionId) {
    if (submissionId == null) {
      throw new InvalidSubmissionIdException("submissionId is null");
    }
    // submissionId is like c01-xxx, get cluster id before '-' character
    int index = submissionId.indexOf('-');
    if (index <= 0) {
      throw new InvalidSubmissionIdException(
          String.format("Invalid submission id: %s", submissionId));
    }
    return submissionId.substring(0, index);
  }

  public static void validateQueueToken(String queue, String queueToken, AppConfig appConfig) {
    if (appConfig.getQueues() != null) {
      final String queueNameToFilter = queue;
      Optional<AppConfig.QueueConfig> queueConfigOptional =
          appConfig.getQueues().stream()
              .filter(q -> q.getName().equalsIgnoreCase(queueNameToFilter))
              .findFirst();
      boolean queueSecure = false;
      if (queueConfigOptional.isPresent()) {
        AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
        if (queueConfig.getSecure() != null) {
          queueSecure = queueConfig.getSecure();
        }
      }
      if (queueSecure) {
        if (queueToken == null || queueToken.isEmpty()) {
          String errorMessage = String.format("Please specify queueToken for queue %s", queue);
          throw new WebApplicationException(errorMessage, Response.Status.BAD_REQUEST);
        }

        if (appConfig.getQueueTokenSOPS() == null) {
          throw new WebApplicationException(
              "Server hit configuration error: no queue token configure",
              Response.Status.INTERNAL_SERVER_ERROR);
        }

        QueueTokenVerifier.verify(queueToken, appConfig.getQueueTokenSOPS().getSecrets(), queue);
      }
    }
  }

  private static String substitutionSparkConfigValue(String value, String submissionId) {
    if (value != null) {
      value = value.replace(Constants.SPARK_APPLICATION_RESOURCE_NAME_VAR, submissionId);
    }
    return value;
  }

  private static Map<String, String> applyFeatureGate(
      SubmitApplicationRequest request, Map<String, String> conf) {
    String featureSwitch = "RangerOnCustomImage";
    List<String> rangerKeys =
        new ArrayList<>(Arrays.asList("spark.sql.extensions", "spark.ranger.plugin.spark.env"));

    Map<String, String> filteredMap =
        conf.entrySet().stream()
            .filter(
                map -> {
                  if (request.getImage() != null
                      && (StringUtils.isEmpty(conf.get(featureSwitch))
                          || !conf.get(featureSwitch).equalsIgnoreCase("On"))) {
                    return !rangerKeys.contains(map.getKey());
                  } else {
                    return true;
                  }
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return filteredMap;
  }

  /**
   * By default all Spark jobs go to the YuniKorn queue root.spark to be gang scheduled
   *
   * @param queue the name of the queue
   * @return BatchSchedulerConfiguration
   */
  public static BatchSchedulerConfiguration getYuniKornSchedulerConfig(String queue) {
    if (queue == null) {
      queue = YUNIKORN_SPARK_DEFAULT_QUEUE;
    }

    BatchSchedulerConfiguration schedulerConfig = new BatchSchedulerConfiguration();

    schedulerConfig.setQueue(YUNIKORN_ROOT_QUEUE + "." + queue);
    schedulerConfig.setGang(false);
    schedulerConfig.setReservationTimeoutInSeconds(PLACEHOLDER_TIMEOUT_IN_SECONDS);

    return schedulerConfig;
  }

  public static DriverSpec getDriverSpec(
      SubmitApplicationRequest request,
      AppConfig appConfig,
      String parentQueue,
      SparkCluster sparkCluster) {
    DriverSpec driverSpec = new DriverSpec();

    if (request.getDriver() != null) {
      driverSpec.copyFrom(request.getDriver());

      Map<String, String> driverLabels = request.getDriver().getLabels();
      if (driverLabels != null) {
        Map<String, String> labels = new HashMap<>();
        if (driverLabels.containsKey(DAG_NAME_LABEL)) {
          labels.put(
              DAG_NAME_LABEL,
              KubernetesHelper.normalizeLabelValue(driverLabels.get(DAG_NAME_LABEL)));
        }
        if (driverLabels.containsKey(TASK_NAME_LABEL)) {
          labels.put(
              TASK_NAME_LABEL,
              KubernetesHelper.normalizeLabelValue(driverLabels.get(TASK_NAME_LABEL)));
        }
        driverLabels.putAll(labels);
        driverSpec.setLabels(driverLabels);
      }
    }

    // if there's any volumes and volume mounts specified, add them to driver spec
    // mounts specified in requests have higher priority
    if (request.getDriver() != null
        && request.getVolumes() != null
        && !request.getVolumes().isEmpty()) {
      driverSpec.setVolumeMounts(request.getDriver().getVolumeMounts());
    } else if (sparkCluster.getDriver() != null
        && sparkCluster.getVolumes() != null
        && !sparkCluster.getVolumes().isEmpty()) {
      driverSpec.setVolumeMounts(sparkCluster.getDriver().getVolumeMounts());
    }

    double driverCpuBufferRatioQueue = getDriverCPUBufferForQueue(appConfig, parentQueue);
    double driverMemBufferRatioQueue = getDriverMemBufferForQueue(appConfig, parentQueue);
    if (driverSpec.getServiceAccount() == null || driverSpec.getServiceAccount().isEmpty()) {
      driverSpec.setServiceAccount(sparkCluster.getSparkServiceAccount());
    }

    int originalDriverCores = driverSpec.getCores();
    long adjustedDriverCores = Math.round(originalDriverCores * driverCpuBufferRatioQueue);
    driverSpec.setCores((int) adjustedDriverCores);
    logger.info("Setting driver core to {}", adjustedDriverCores);

    if (driverSpec.getCoreLimit() == null || driverSpec.getCoreLimit().isEmpty()) {
      driverSpec.setCoreLimit(
          String.format(
              "%sm",
              Math.ceil(driverSpec.getCores() * CORE_LIMIT_RATIO * driverCpuBufferRatioQueue)));
    } else {
      long originalDriverCoreLimit = Long.parseLong(driverSpec.getCoreLimit());
      long adjustedDriverCoreLimit =
          Math.round(originalDriverCoreLimit * driverCpuBufferRatioQueue);
      driverSpec.setCoreLimit(String.valueOf(adjustedDriverCoreLimit));
      logger.info("Setting driver core limits to {}", adjustedDriverCoreLimit);
    }

    long originalDriverMem = getMemNumFromRequestStr(driverSpec.getMemory());
    long adjustedDriverMem = (long) Math.ceil(originalDriverMem * driverMemBufferRatioQueue);
    String memUnit = getMemUnitFromRequestStr(driverSpec.getMemory());
    driverSpec.setMemory(adjustedDriverMem + memUnit);
    logger.info("Setting driver memory to {}{}", adjustedDriverMem, memUnit);

    // set annotations to driver pod to avoid scale-in from KubeC
    Map<String, String> driverAnnotations =
        driverSpec.getAnnotations() == null ? new HashMap<>() : driverSpec.getAnnotations();
    driverAnnotations.put(
        KUBE_CLUSTER_AUTOSCALER_SCALE_IN_ANNOTATION_KEY,
        KUBE_CLUSTER_AUTOSCALER_SCALE_IN_ANNOTATION_VALUE);
    driverSpec.setAnnotations(driverAnnotations);

    // Driver affinity is for all drivers to share the same node group, with only scaling up
    // this is to prevent driver pods from being killed when nodes are being scaling down
    if (driverSpec.getAffinity() == null) {
      List<String> driverNodeLabelValues = getDriverNodeLabelValuesForQueue(appConfig, parentQueue);
      String nodeLabelKey = getDriverNodeLabelKeyForQueue(appConfig, parentQueue);
      if (driverNodeLabelValues != null && driverNodeLabelValues.size() > 0) {
        // set hard requiredDuringSchedulingIgnoredDuringExecutionTerm to driver
        NodeSelectorRequirement nodeSelectorRequirement =
            new NodeSelectorRequirement(
                nodeLabelKey,
                NodeSelectorOperator.NodeSelectorOpIn.toString(),
                driverNodeLabelValues.toArray(new String[0])); // set "spark_driver" as nodeselctor

        NodeSelectorTerm nodeSelectorTermOnDriver =
            new NodeSelectorTerm(new NodeSelectorRequirement[] {nodeSelectorRequirement});

        RequiredDuringSchedulingIgnoredDuringExecutionTerm requiredSchedulingTerm =
            new RequiredDuringSchedulingIgnoredDuringExecutionTerm(
                new NodeSelectorTerm[] {nodeSelectorTermOnDriver});

        // attach required affinity only to driver
        NodeAffinity nodeAffinity = new NodeAffinity(requiredSchedulingTerm);
        driverSpec.setAffinity(new Affinity(nodeAffinity));
      }
    }

    return driverSpec;
  }

  private static double getDriverCPUBufferForQueue(AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getDriverCPUBufferRatio() != null) {
        return queueConfig.getDriverCPUBufferRatio();
      }
    }
    return DRIVER_CPU_BUFFER_RATIO;
  }

  private static double getDriverMemBufferForQueue(AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getDriverMemBufferRatio() != null) {
        return queueConfig.getDriverMemBufferRatio();
      }
    }
    return DRIVER_MEM_BUFFER_RATIO;
  }

  private static String getDriverNodeLabelKeyForQueue(AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getDriverNodeLabelKey() != null) {
        return queueConfig.getDriverNodeLabelKey();
      }
    }
    return DEFAULT_DRIVER_NODE_LABEL_KEY;
  }

  private static List<String> getDriverNodeLabelValuesForQueue(AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();
    List<String> res = new ArrayList<>();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getDriverNodeLabelValues() != null) {
        res.addAll(queueConfig.getDriverNodeLabelValues());
      }
    }
    return res;
  }

  private static double getExecutorMemBufferForQueue(AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getExecutorMemBufferRatio() != null) {
        return queueConfig.getExecutorMemBufferRatio();
      }
    }
    return EXECUTOR_MEM_BUFFER_RATIO;
  }

  private static double getExecutorCPUBufferForQueue(AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getExecutorCPUBufferRatio() != null) {
        return queueConfig.getExecutorCPUBufferRatio();
      }
    }
    return EXECUTOR_CPU_BUFFER_RATIO;
  }

  private static List<String> getExecutorSpotNodeLabelValuesForQueue(
      AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();
    List<String> res = new ArrayList<>();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getExecutorSpotNodeLabelValues() != null) {
        res.addAll(queueConfig.getExecutorSpotNodeLabelValues());
      }
    }
    return res;
  }

  private static String getExecutorNodeLabelKeyForQueue(AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getExecutorNodeLabelKey() != null) {
        return queueConfig.getExecutorNodeLabelKey();
      }
    }
    return DEFAULT_EXECUTOR_NODE_LABEL_KEY;
  }

  private static List<String> getExecutorNodeLabelValuesForQueue(
      AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();
    List<String> res = new ArrayList<>();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getExecutorNodeLabelValues() != null) {
        res.addAll(queueConfig.getExecutorNodeLabelValues());
      }
    }
    return res;
  }

  private static long getMemNumFromRequestStr(String memStr) {
    int unitIndex = getMemUnitIndexFromRequestStr(memStr);
    String memNumStr = memStr.substring(0, unitIndex);
    return (long) Double.parseDouble(memNumStr);
  }

  private static String getMemUnitFromRequestStr(String memStr) {
    int unitIndex = getMemUnitIndexFromRequestStr(memStr);
    return memStr.substring(unitIndex);
  }

  private static int getMemUnitIndexFromRequestStr(String memStr) {
    int unitIndex = -1;
    for (int i = 0; i < memStr.length(); i++) {
      if (!Character.isDigit(memStr.charAt(i)) && memStr.charAt(i) != '.') {
        unitIndex = i;
        break;
      }
    }
    return unitIndex;
  }

  public static ExecutorSpec getExecutorSpec(
      SubmitApplicationRequest request,
      AppConfig appConfig,
      String parentQueue,
      SparkCluster sparkCluster) {
    ExecutorSpec executorSpec = new ExecutorSpec();

    if (request.getExecutor() != null) {
      executorSpec.copyFrom(request.getExecutor());

      Map<String, String> executorLabels = request.getExecutor().getLabels();
      if (executorLabels != null) {
        Map<String, String> labels = new HashMap<>();
        if (executorLabels.containsKey(DAG_NAME_LABEL)) {
          labels.put(
              DAG_NAME_LABEL,
              KubernetesHelper.normalizeLabelValue(executorLabels.get(DAG_NAME_LABEL)));
        }
        if (executorLabels.containsKey(TASK_NAME_LABEL)) {
          labels.put(
              TASK_NAME_LABEL,
              KubernetesHelper.normalizeLabelValue(executorLabels.get(TASK_NAME_LABEL)));
        }
        executorLabels.putAll(labels);
        executorSpec.setLabels(executorLabels);
      }
    }

    // if there's any volumes and volume mounts specified, add them to executor spec
    // mounts specified in requests have higher priority
    if (request.getExecutor() != null
        && request.getVolumes() != null
        && !request.getVolumes().isEmpty()) {
      executorSpec.setVolumeMounts(request.getExecutor().getVolumeMounts());
    } else if (sparkCluster.getExecutor() != null
        && sparkCluster.getVolumes() != null
        && !sparkCluster.getVolumes().isEmpty()) {
      executorSpec.setVolumeMounts(sparkCluster.getExecutor().getVolumeMounts());
    }

    double executorCpuBufferRatioQueue = getExecutorCPUBufferForQueue(appConfig, parentQueue);
    double executorMemBufferRatioQueue = getExecutorMemBufferForQueue(appConfig, parentQueue);

    int originalExecutorCores = executorSpec.getCores();
    long adjustedExecutorCores = Math.round(originalExecutorCores * executorCpuBufferRatioQueue);
    executorSpec.setCores((int) adjustedExecutorCores);
    logger.info("Setting executor core to {}", adjustedExecutorCores);

    if (executorSpec.getCoreLimit() == null || executorSpec.getCoreLimit().isEmpty()) {
      executorSpec.setCoreLimit(
          String.format(
              "%sm",
              Math.ceil(executorSpec.getCores() * CORE_LIMIT_RATIO * executorCpuBufferRatioQueue)));
    } else {
      long originalExecutorCoreLimit = Long.parseLong(executorSpec.getCoreLimit());
      long adjustedExecutorCoreLimit =
          Math.round(originalExecutorCoreLimit * executorCpuBufferRatioQueue);
      executorSpec.setCoreLimit(String.valueOf(adjustedExecutorCoreLimit));
      logger.info("Setting executor core limits to {}", adjustedExecutorCoreLimit);
    }

    long originalExecutorMem = getMemNumFromRequestStr(executorSpec.getMemory());
    long adjustedExecutorMem = (long) Math.ceil(originalExecutorMem * executorMemBufferRatioQueue);
    String memUnit = getMemUnitFromRequestStr(executorSpec.getMemory());
    executorSpec.setMemory(adjustedExecutorMem + memUnit);
    logger.info("Setting executor memory to {}{}", adjustedExecutorMem, memUnit);

    // set annotations to executor pod to avoid scale-in from KubeCA
    if (executorSpec.getAnnotations() == null) {
      Map<String, String> executorAnnotations = new HashMap<>();
      executorAnnotations.put(
          KUBE_CLUSTER_AUTOSCALER_SCALE_IN_ANNOTATION_KEY,
          KUBE_CLUSTER_AUTOSCALER_SCALE_IN_ANNOTATION_VALUE);
      executorSpec.setAnnotations(executorAnnotations);
    }

    if (executorSpec.getAffinity() == null) {
      List<String> executorNodeLabelValues = new ArrayList<>();

      if (request.getSpotInstance()) {
        executorNodeLabelValues = getExecutorSpotNodeLabelValuesForQueue(appConfig, parentQueue);
      } else {
        executorNodeLabelValues = getExecutorNodeLabelValuesForQueue(appConfig, parentQueue);
      }

      String nodeLabelKey = getExecutorNodeLabelKeyForQueue(appConfig, parentQueue);
      if (executorNodeLabelValues != null && executorNodeLabelValues.size() > 0) {
        // set hard requiredDuringSchedulingIgnoredDuringExecutionTerm to executor
        NodeSelectorRequirement nodeSelectorRequirement =
            new NodeSelectorRequirement(
                nodeLabelKey,
                NodeSelectorOperator.NodeSelectorOpIn.toString(),
                executorNodeLabelValues.toArray(new String[0]));

        NodeSelectorTerm nodeSelectorTermOnExecutor =
            new NodeSelectorTerm(new NodeSelectorRequirement[] {nodeSelectorRequirement});

        RequiredDuringSchedulingIgnoredDuringExecutionTerm requiredSchedulingTermOnExecutor =
            new RequiredDuringSchedulingIgnoredDuringExecutionTerm(
                new NodeSelectorTerm[] {nodeSelectorTermOnExecutor});

        // attach required affinity only to executor
        NodeAffinity nodeAffinityOnExecutor = new NodeAffinity(requiredSchedulingTermOnExecutor);
        executorSpec.setAffinity(new Affinity(nodeAffinityOnExecutor));
      }
    }

    PodDNSConfig executorPodDNSConfig = new PodDNSConfig();
    executorPodDNSConfig.setOptions(
        Collections.singletonList(
            new PodDNSConfigOption(DNS_CONFIG_OPTION_NDOTS_NAME, DNS_CONFIG_OPTION_NDOTS_VALUE)));
    executorSpec.setDnsConfig(executorPodDNSConfig);

    return executorSpec;
  }

  public static String getType(String specifiedType, String mainClass) {
    if (specifiedType != null && !specifiedType.isEmpty()) {
      return specifiedType;
    }

    if (mainClass != null && !mainClass.isEmpty()) {
      return Constants.JAVA_TYPE;
    }

    return Constants.PYTHON_TYPE;
  }

  public static String getImage(
      AppConfig appConfig,
      SubmitApplicationRequest request,
      String type,
      String sparkVersion,
      String proxyUser) {
    if (request.getImage() == null || request.getImage().isEmpty()) {
      Optional<AppConfig.SparkImage> sparkImage = appConfig.resolveImage(type, sparkVersion);
      if (!sparkImage.isPresent()) {
        throw new WebApplicationException(
            String.format("Spark image not found for type: %s, version: %s", type, sparkVersion),
            Response.Status.BAD_REQUEST);
      }
      return sparkImage.get().getName();

    } else {
      logger.info("User {} is using a custom spark image {}", proxyUser, request.getImage());

      return request.getImage();
    }
  }
}
