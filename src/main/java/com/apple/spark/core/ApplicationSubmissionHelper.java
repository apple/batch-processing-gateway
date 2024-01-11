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
import static com.apple.spark.core.MonitoringConstants.*;
import static com.apple.spark.core.SparkConstants.*;
import static com.apple.spark.core.SparkPodNodeAffinityHelper.*;
import static com.apple.spark.core.SparkPodNodeAffinityHelper.createNodeSelectorTermForSparkPods;

import com.apple.spark.AppConfig;
import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.crd.VirtualSparkClusterSpec;
import com.apple.spark.operator.*;
import com.apple.spark.util.ExceptionUtils;
import com.apple.spark.util.FileUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.fabric8.kubernetes.api.model.PodDNSConfig;
import io.fabric8.kubernetes.api.model.PodDNSConfigOption;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
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
    if (AIRFLOW_SYSTEM_ACCOUNTS.contains(user) || FEATURESTORE_SYSTEM_ACCOUNTS.contains(user)) {
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
      Map<String, String> fixedSparkConf,
      VirtualSparkClusterSpec sparkCluster) {

    Map<String, String> sparkConf = null;

    if (defaultSparkConf != null) {
      if (sparkConf == null) {
        sparkConf = new HashMap<>();
      }

      for (Map.Entry<String, String> entry : defaultSparkConf.entrySet()) {
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
      sparkConf.putAll(request.getSparkConf());
    }

    if (fixedSparkConf != null) {
      if (sparkConf == null) {
        sparkConf = new HashMap<>();
      }
      sparkConf.putAll(fixedSparkConf);
    }

    if (!StringUtils.isEmpty(request.getApplicationName())) {
      if (sparkConf == null) {
        sparkConf = new HashMap<>();
      }
      sparkConf.put(SparkConstants.SPARK_APP_NAME_CONFIG, request.getApplicationName());
    }

    return sparkConf;
  }

  /**
   * This is to apply the Spark conf to enable query listener used by CDO team This requires the
   * Spark images to be the default (or newer) images provided by AIML:
   * docker.apple.com/aiml-di-dpi/spark/branch-3.2.0-aiml-spark/spark:aiml-pyspark-3.2.0.66
   * docker.apple.com/aiml-di-dpi/spark/branch-3.2.0-aiml-spark/spark:aiml-spark-3.2.0.51
   *
   * @param sparkSpec the spark spec to be populated
   */
  public static void applyQueryListenerSparkConf(SparkApplicationSpec sparkSpec) {
    if (sparkSpec.getSparkConf() == null) {
      sparkSpec.setSparkConf(new HashMap<>());
    }

    sparkSpec
        .getSparkConf()
        .put(
            "spark.sql.queryExecutionListeners",
            "com.apple.aiml.iceberg.usage.listener.query_execution.QueryExecutionListenerCDO");
  }

  public static SparkUIConfiguration getSparkUIConfiguration(
      String submissionId, VirtualSparkClusterSpec sparkCluster) {
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
      SubmitApplicationRequest request, VirtualSparkClusterSpec sparkCluster) {

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

  public static List<VolumeMount> getVolumeMounts(
      SubmitApplicationRequest request, VirtualSparkClusterSpec sparkCluster) {

    // if there are volume mounts in the request, use that to populate spark spec
    if (request.getVolumeMounts() != null && !request.getVolumeMounts().isEmpty()) {
      return request.getVolumeMounts();
    }

    // if there are volume mounts in the spark cluster, use that to populate spark spec
    if (sparkCluster.getVolumeMounts() != null && !sparkCluster.getVolumeMounts().isEmpty()) {
      return sparkCluster.getVolumeMounts();
    }

    return null;
  }

  public static void populateEnv(
      SparkApplicationSpec sparkSpec,
      SubmitApplicationRequest request,
      VirtualSparkClusterSpec sparkCluster) {
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
      VirtualSparkClusterSpec sparkCluster) {
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
              (int)
                  Math.ceil(driverSpec.getCores() * CORE_LIMIT_RATIO * driverCpuBufferRatioQueue)));
    } else {
      double originalDriverCoreLimit = getNumFromRequestStr(driverSpec.getCoreLimit());
      String driverCoreLimitUnit = getUnitFromRequestStr(driverSpec.getCoreLimit());
      double adjustedDriverCoreLimit =
          Math.round(originalDriverCoreLimit * driverCpuBufferRatioQueue);
      if (driverCoreLimitUnit.equals("m")) {
        driverSpec.setCoreLimit((long) adjustedDriverCoreLimit + driverCoreLimitUnit);
      } else {
        driverSpec.setCoreLimit(adjustedDriverCoreLimit + driverCoreLimitUnit);
      }
      logger.info(
          "Setting driver core limits to {}", adjustedDriverCoreLimit + driverCoreLimitUnit);
    }

    if (driverSpec.getCoreRequest() == null || driverSpec.getCoreRequest().isEmpty()) {
      driverSpec.setCoreRequest(
          String.format(
              "%sm",
              (int)
                  Math.ceil(
                      driverSpec.getCores() * CORE_REQUEST_RATIO * driverCpuBufferRatioQueue)));
    } else {
      double originalDriverCoreRequest = getNumFromRequestStr(driverSpec.getCoreRequest());
      String driverCoreRequestUnit = getUnitFromRequestStr(driverSpec.getCoreRequest());
      double adjustedDriverCoreRequest =
          Math.round(originalDriverCoreRequest * driverCpuBufferRatioQueue);
      if (driverCoreRequestUnit.equals("m")) {
        driverSpec.setCoreRequest((long) adjustedDriverCoreRequest + driverCoreRequestUnit);
      } else {
        driverSpec.setCoreRequest(adjustedDriverCoreRequest + driverCoreRequestUnit);
      }
      logger.info(
          "Setting driver core request to {}", adjustedDriverCoreRequest + driverCoreRequestUnit);
    }

    long originalDriverMem = (long) getNumFromRequestStr(driverSpec.getMemory());
    long adjustedDriverMem = (long) Math.ceil(originalDriverMem * driverMemBufferRatioQueue);
    String memUnit = getUnitFromRequestStr(driverSpec.getMemory());
    driverSpec.setMemory(adjustedDriverMem + memUnit);
    logger.info("Setting driver memory to {}{}", adjustedDriverMem, memUnit);

    // set annotations to driver pod to avoid scale-in from KubeC
    Map<String, String> driverAnnotations =
        driverSpec.getAnnotations() == null ? new HashMap<>() : driverSpec.getAnnotations();
    driverAnnotations.put(
        KUBE_CLUSTER_AUTOSCALER_SCALE_IN_ANNOTATION_KEY,
        KUBE_CLUSTER_AUTOSCALER_SCALE_IN_ANNOTATION_VALUE);
    driverAnnotations.put(KAPENTER_SCALE_IN_ANNOTATION_KEY, KAPENTER_SCALE_IN_ANNOTATION_VALUE);
    driverSpec.setAnnotations(driverAnnotations);

    // Driver affinity is for all drivers to share the same node group, with only scaling up
    // this is to prevent driver pods from being killed when nodes are being scaling down
    if (driverSpec.getAffinity() == null) {
      // attach required affinity only to executor
      NodeSelectorTerm nodeSelectorTermOnDriver =
          createNodeSelectorTermForSparkPods(
              appConfig,
              parentQueue,
              true,
              !request.getSpotInstance(),
              request.getNodeArch(),
              isGpuEnabled(request));
      if (nodeSelectorTermOnDriver != null
          && nodeSelectorTermOnDriver.getMatchExpressions() != null) {
        RequiredDuringSchedulingIgnoredDuringExecutionTerm requiredSchedulingTermOnDriver =
            new RequiredDuringSchedulingIgnoredDuringExecutionTerm(
                new NodeSelectorTerm[] {nodeSelectorTermOnDriver});

        NodeAffinity nodeAffinityOnDriver = new NodeAffinity(requiredSchedulingTermOnDriver);
        driverSpec.setAffinity(new Affinity(nodeAffinityOnDriver));
      }
    }

    Map<String, String> sparkConf = request.getSparkConf();
    if (sparkConf != null) {
      String driverJavaOptions = sparkConf.get("spark.driver.extraJavaOptions");
      if (driverJavaOptions != null && driverJavaOptions.length() > 0) {
        driverSpec.setJavaOptions(driverJavaOptions);
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

  private static double getNumFromRequestStr(String resourcesStr) {
    int unitIndex = getUnitIndexFromRequestStr(resourcesStr);
    String numStr = resourcesStr.substring(0, unitIndex);
    if (numStr.length() == 0) return 0.0;
    return Double.parseDouble(numStr);
  }

  private static String getUnitFromRequestStr(String resourcesStr) {
    int unitIndex = getUnitIndexFromRequestStr(resourcesStr);
    return resourcesStr.substring(unitIndex);
  }

  private static int getUnitIndexFromRequestStr(String resourcesStr) {
    int unitIndex = -1;
    for (int i = 0; i < resourcesStr.length(); i++) {
      if (!Character.isDigit(resourcesStr.charAt(i)) && resourcesStr.charAt(i) != '.') {
        unitIndex = i;
        break;
      }
    }
    return unitIndex == -1 ? resourcesStr.length() : unitIndex;
  }

  public static ExecutorSpec getExecutorSpec(
      SubmitApplicationRequest request,
      AppConfig appConfig,
      String parentQueue,
      VirtualSparkClusterSpec sparkCluster) {
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
              (int)
                  Math.ceil(
                      executorSpec.getCores() * CORE_LIMIT_RATIO * executorCpuBufferRatioQueue)));
    } else {
      double originalExecutorCoreLimit = getNumFromRequestStr(executorSpec.getCoreLimit());
      String executorCoreLimitUnit = getUnitFromRequestStr(executorSpec.getCoreLimit());
      double adjustedExecutorCoreLimit =
          Math.round(originalExecutorCoreLimit * executorCpuBufferRatioQueue);
      if (executorCoreLimitUnit.equals("m")) {
        executorSpec.setCoreLimit((long) adjustedExecutorCoreLimit + executorCoreLimitUnit);
      } else {
        executorSpec.setCoreLimit(adjustedExecutorCoreLimit + executorCoreLimitUnit);
      }
      logger.info(
          "Setting executor core limits to {}", adjustedExecutorCoreLimit + executorCoreLimitUnit);
    }

    if (executorSpec.getCoreRequest() == null || executorSpec.getCoreRequest().isEmpty()) {
      executorSpec.setCoreRequest(
          String.format(
              "%sm",
              (int)
                  Math.ceil(
                      executorSpec.getCores() * CORE_REQUEST_RATIO * executorCpuBufferRatioQueue)));
    } else {

      double originalExecutorCoreRequest = getNumFromRequestStr(executorSpec.getCoreRequest());
      String executorCoreRequestUnit = getUnitFromRequestStr(executorSpec.getCoreRequest());
      double adjustedExecutorCoreRequest =
          Math.round(originalExecutorCoreRequest * executorCpuBufferRatioQueue);
      if (executorCoreRequestUnit.equals("m")) {
        executorSpec.setCoreRequest((long) adjustedExecutorCoreRequest + executorCoreRequestUnit);
      } else {
        executorSpec.setCoreRequest(adjustedExecutorCoreRequest + executorCoreRequestUnit);
      }
      logger.info(
          "Setting executor core request to {}",
          adjustedExecutorCoreRequest + executorCoreRequestUnit);
    }

    long originalExecutorMem = (long) getNumFromRequestStr(executorSpec.getMemory());
    long adjustedExecutorMem = (long) Math.ceil(originalExecutorMem * executorMemBufferRatioQueue);
    String memUnit = getUnitFromRequestStr(executorSpec.getMemory());
    executorSpec.setMemory(adjustedExecutorMem + memUnit);
    logger.info("Setting executor memory to {}{}", adjustedExecutorMem, memUnit);

    // set annotations to executor pod to avoid scale-in from KubeCA
    if (executorSpec.getAnnotations() == null) {
      Map<String, String> executorAnnotations = new HashMap<>();
      executorAnnotations.put(
          KUBE_CLUSTER_AUTOSCALER_SCALE_IN_ANNOTATION_KEY,
          KUBE_CLUSTER_AUTOSCALER_SCALE_IN_ANNOTATION_VALUE);
      executorAnnotations.put(KAPENTER_SCALE_IN_ANNOTATION_KEY, KAPENTER_SCALE_IN_ANNOTATION_VALUE);
      executorSpec.setAnnotations(executorAnnotations);
    }

    if (executorSpec.getAffinity() == null) {
      // attach required affinity only to executor
      NodeSelectorTerm nodeSelectorTermOnExecutor =
          createNodeSelectorTermForSparkPods(
              appConfig,
              parentQueue,
              false,
              !request.getSpotInstance(),
              request.getNodeArch(),
              isGpuEnabled(request));

      if (nodeSelectorTermOnExecutor != null
          && nodeSelectorTermOnExecutor.getMatchExpressions() != null) {
        RequiredDuringSchedulingIgnoredDuringExecutionTerm requiredSchedulingTermOnExecutor =
            new RequiredDuringSchedulingIgnoredDuringExecutionTerm(
                new NodeSelectorTerm[] {nodeSelectorTermOnExecutor});

        NodeAffinity nodeAffinityOnExecutor = new NodeAffinity(requiredSchedulingTermOnExecutor);
        executorSpec.setAffinity(new Affinity(nodeAffinityOnExecutor));
      }
    }

    PodDNSConfig executorPodDNSConfig = new PodDNSConfig();
    executorPodDNSConfig.setOptions(
        Collections.singletonList(
            new PodDNSConfigOption(DNS_CONFIG_OPTION_NDOTS_NAME, DNS_CONFIG_OPTION_NDOTS_VALUE)));
    executorSpec.setDnsConfig(executorPodDNSConfig);

    Map<String, String> sparkConf = request.getSparkConf();
    if (sparkConf != null) {
      String executorJavaOptions = sparkConf.get("spark.executor.extraJavaOptions");
      if (executorJavaOptions != null && executorJavaOptions.length() > 0) {
        executorSpec.setJavaOptions(executorJavaOptions);
      }
    }

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

  public static void populatePrometheusMonitoring(SubmitApplicationRequest request) {
    if (request.getMonitoring() == null) {
      request.setMonitoring(new MonitoringSpec());
    }
    MonitoringSpec monitoring = request.getMonitoring();
    if (monitoring.getPrometheus() != null) {
      return;
    }
    try {
      // Parse the Spark prometheus config file to promSpec
      // If spark version not supported, will throw NullPointerException
      String prometheusConfig =
          FileUtil.readFileAsString(getPrometheusConfigPath(request.getSparkVersion()));
      PrometheusSpec promSpec = new PrometheusSpec();
      promSpec.setConfiguration(prometheusConfig);
      promSpec.setJmxExporterJar(PROMETHEUS_JAR_PATH);
      promSpec.setPort(DEFAULT_PROMETHEUS_PORT);
      monitoring.setPrometheus(promSpec);
      monitoring.setExposeDriverMetrics(true);
      monitoring.setExposeExecutorMetrics(true);
    } catch (Throwable e) {
      throw new WebApplicationException(e);
    }
  }

  static String getPrometheusConfigPath(String sparkVersion) {
    if (sparkVersion.startsWith("3")) {
      return PROMETHEUS_SPARK3_CONFIG_PATH;
    } else if (sparkVersion.startsWith("2")) {
      return PROMETHEUS_SPARK2_CONFIG_PATH;
    }
    return null;
  }

  public static void populatePrometheusAnnotations(SubmitApplicationRequest request) {
    // This assumes Prometheus Spec is set
    if (request.getMonitoring().getPrometheus().getPort() == null) {
      request.getMonitoring().getPrometheus().setPort(DEFAULT_PROMETHEUS_PORT);
    }
    int port = request.getMonitoring().getPrometheus().getPort();

    if (request.getDriver().getAnnotations() == null) {
      request.getDriver().setAnnotations(new HashMap<>());
    }
    Map<String, String> driverAnnotations = request.getDriver().getAnnotations();
    driverAnnotations.putAll(getDatadogAnnotations(DRIVER_CONTAINER_NAME, port));

    if (request.getExecutor().getAnnotations() == null) {
      request.getExecutor().setAnnotations(new HashMap<>());
    }
    Map<String, String> executorAnnotations = request.getExecutor().getAnnotations();
    executorAnnotations.putAll(getDatadogAnnotations(EXECUTOR_CONTAINER_NAME, port));
  }

  /**
   * ad.datadoghq.com/spark-kubernetes-{driver|executor}.check_names=["openmetrics"]
   * ad.datadoghq.com/spark-kubernetes-{driver|executor}.init_configs=[{}]
   * ad.datadoghq.com/spark-kubernetes-{driver|executor}.instances=[{ "prometheus_url":
   * "http://%%host%%:9889/metrics", "namespace": "spark", "metrics": ["*"] }]
   */
  static Map<String, String> getDatadogAnnotations(String containerName, int port) {
    String datadogPrefix = String.format("%s/%s", DATADOG_AD_PREFIX, containerName);
    return Map.ofEntries(
        Map.entry(String.format("%s.%s", datadogPrefix, "check_names"), "[\"openmetrics\"]"),
        Map.entry(String.format("%s.%s", datadogPrefix, "init_configs"), "[{}]"),
        Map.entry(
            String.format("%s.%s", datadogPrefix, "instances"),
            String.format(
                "[{\"prometheus_url\":\"http://%%%%host%%%%:%d/%s\","
                    + "\"namespace\":\"spark\",\"metrics\":[\"*\"]}]",
                port, DEFAULT_PROMETHEUS_METRICS_PATH)));
  }
}
