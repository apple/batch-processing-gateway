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

import static com.apple.spark.core.Constants.*;

import com.apple.spark.AppConfig;
import com.apple.spark.operator.DriverSpec;
import com.apple.spark.operator.ExecutorSpec;
import com.apple.spark.operator.SparkApplicationResource;
import com.apple.spark.operator.SparkApplicationResourceList;
import com.apple.spark.operator.SparkApplicationSpec;
import com.apple.spark.util.CounterMetricContainer;
import com.apple.spark.util.DateTimeUtils;
import com.apple.spark.util.GaugeMetricContainer;
import com.apple.spark.util.KubernetesClusterAndNamespace;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.dsl.base.OperationContext;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.internal.KubernetesDeserializer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application monitor is a separate component that runs on the side to keep the DB informed of all
 * the latest job status, and monitor the running jobs (currently killing long-running jobs).
 */
public class ApplicationMonitor implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ApplicationMonitor.class);

  private static final long RESYNC_MILLIS = 3 * 60 * 1000L;

  // Keep a list of submission_ids whose info have been inserted into the database.
  // This helps avoid keeping updating the database upon receiving an event.
  private static final HashSet<String> driverInfoCollectedSubIds = new HashSet<String>();
  private final CounterMetricContainer requestCounters;
  private final GaugeMetricContainer gaugeMetricContainer;
  private final AppConfig appConfig;
  private final LogDao logDao;
  private final List<KubernetesClient> clients = new ArrayList<>();
  private final List<SharedInformerFactory> informerFactories = new ArrayList<>();
  private final MeterRegistry meterRegistry;

  // The event queue of all the application updates.
  // One thread will be launched to keep processing the events from the queue,
  // and CRD informers will put events into the queue when there are CRD state updates.
  private final ArrayBlockingQueue<ApplicationUpdateEvent> applicationUpdateEventQueue =
      new ArrayBlockingQueue<>(Constants.APPLICATION_MONITOR_QUEUE_CAPACITY);
  private final AtomicLong droppedEvents = new AtomicLong();

  public ApplicationMonitor(AppConfig appConfig, MeterRegistry meterRegistry) {
    this.appConfig = appConfig;

    String dbConnectionString = null;
    String dbUser = null;
    String dbPassword = null;
    String dbName = null;

    if (appConfig.getDbStorageSOPS() != null) {
      dbConnectionString = appConfig.getDbStorageSOPS().getConnectionString();
      dbUser = appConfig.getDbStorageSOPS().getUser();
      dbPassword = appConfig.getDbStorageSOPS().getPassword();
      dbName = appConfig.getDbStorageSOPS().getDbName();
    }

    this.logDao = new LogDao(dbConnectionString, dbUser, dbPassword, dbName, meterRegistry);
    this.requestCounters = new CounterMetricContainer(meterRegistry);

    this.gaugeMetricContainer = new GaugeMetricContainer(meterRegistry);
    this.gaugeMetricContainer.register(
        MONITOR_QUEUE_SIZE, () -> this.applicationUpdateEventQueue.size());

    this.meterRegistry = meterRegistry;
  }

  /**
   * Launch a thread that updates DB with the latest job state, and then launch one
   * RunningApplicationMonitor instance for each Spark cluster
   */
  public void start() {
    // Launch a thread that updates DB with the latest job state, and notifies the
    // RunningApplicationMonitor of the Spark cluster the event came from.
    String threadName = this.getClass().getSimpleName();
    Thread thread =
        new Thread(
            () -> {
              logger.info("Thread started: {}", threadName);
              int timeoutMillis = 1000;
              while (true) {
                try {
                  ApplicationUpdateEvent event =
                      applicationUpdateEventQueue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
                  if (event != null) {
                    try {
                      // Update DB with job status, metadata and applicationId
                      onUpdateImpl_logApplication(
                          event.getSparkCluster(), event.getPrevCRDState(), event.getNewCRDState());
                    } catch (Throwable ex) {
                      logger.warn("Failed to run onUpdate to log application", ex);
                    }

                    try {
                      // notify the RunningApplicationMonitor of the new object
                      event
                          .getRunningApplicationMonitor()
                          .onUpdate(event.getPrevCRDState(), event.getNewCRDState());
                    } catch (Throwable ex) {
                      logger.warn("Failed to run onUpdate on running application monitor", ex);
                    }
                  }
                } catch (Throwable e) {
                  logger.warn("Failed to dequeue and process event", e);
                }
              }
            });
    thread.setDaemon(true);
    thread.setName(threadName);
    thread.start();

    KubernetesDeserializer.registerCustomKind(
        String.format(
            "%s/%s", SparkConstants.SPARK_APPLICATION_CRD_GROUP, SparkConstants.CRD_VERSION),
        SparkConstants.SPARK_APPLICATION_KIND,
        SparkApplicationResource.class);

    if (appConfig.getSparkClusters() != null) {
      Map<KubernetesClusterAndNamespace, AppConfig.SparkCluster> uniqueClusters = new HashMap<>();
      for (AppConfig.SparkCluster sparkCluster : appConfig.getSparkClusters()) {
        if (sparkCluster.getWeight() > 0) {
          KubernetesClusterAndNamespace kubernetesClusterAndNamespace =
              new KubernetesClusterAndNamespace(
                  sparkCluster.getMasterUrl(), sparkCluster.getSparkApplicationNamespace());

          uniqueClusters.put(kubernetesClusterAndNamespace, sparkCluster);
        }
      }

      // For each Spark cluster, start a RunningApplicationMonitor instance, and register a CRD
      // informer to listen to CRD updates.
      Timer timer = new Timer(true);
      for (AppConfig.SparkCluster sparkCluster : uniqueClusters.values()) {
        start(sparkCluster, timer);
      }
    }
  }

  /**
   * For the given Spark cluster, start a runningApplicationMonitor, and then register a CRD
   * informer to keep track of all CRD updates. The updates are put into the
   * applicationUpdateEventQueue for processing later.
   *
   * @param sparkCluster Spark cluster
   * @param timer timer to set up runningApplicationMonitor
   */
  private void start(AppConfig.SparkCluster sparkCluster, Timer timer) {
    logger.info(
        "Creating informer for spark cluster {}, EKS: {}, namespace: {}",
        sparkCluster.getId(),
        sparkCluster.getEksCluster(),
        sparkCluster.getSparkApplicationNamespace());

    DefaultKubernetesClient client = KubernetesHelper.getK8sClient(sparkCluster);
    clients.add(client);

    SharedInformerFactory sharedInformerFactory = client.informers();
    informerFactories.add(sharedInformerFactory);

    CustomResourceDefinitionContext crdContext = KubernetesHelper.getSparkApplicationCrdContext();
    SharedIndexInformer<SparkApplicationResource> informer =
        sharedInformerFactory.sharedIndexInformerForCustomResource(
            crdContext,
            SparkApplicationResource.class,
            SparkApplicationResourceList.class,
            new OperationContext().withNamespace(sparkCluster.getSparkApplicationNamespace()),
            RESYNC_MILLIS);

    RunningApplicationMonitor runningApplicationMonitor =
        new RunningApplicationMonitor(sparkCluster, timer, meterRegistry);

    informer.addEventHandler(
        new ResourceEventHandler<SparkApplicationResource>() {
          @Override
          public void onAdd(SparkApplicationResource sparkApplicationResource) {}

          @Override
          public void onUpdate(
              SparkApplicationResource prevCRDState, SparkApplicationResource newCRDState) {
            int timeoutMillis = 100;
            try {
              boolean added =
                  applicationUpdateEventQueue.offer(
                      new ApplicationUpdateEvent(
                          sparkCluster, prevCRDState, newCRDState, runningApplicationMonitor),
                      timeoutMillis,
                      TimeUnit.MILLISECONDS);
              if (!added) {
                requestCounters.increment(MONITOR_DROPPED_EVENT);
                long value = droppedEvents.incrementAndGet();
                logger.warn(
                    "Failed to enqueue application update event due to queue full, application: {},"
                        + " total dropped: {}",
                    prevCRDState.getMetadata().getName(),
                    value);
              }
            } catch (Throwable e) {
              logger.warn("Failed to enqueue application update event", e);
            }
          }

          @Override
          public void onDelete(
              SparkApplicationResource sparkApplicationResource,
              boolean deletedFinalStateUnknown) {}
        });

    sharedInformerFactory.addSharedInformerEventListener(
        ex ->
            logger.warn(
                String.format(
                    "Got exception with informer for spark cluster %s", sparkCluster.getId()),
                ex));

    logger.info("Starting all registered informers");
    sharedInformerFactory.startAllRegisteredInformers();
  }

  @Override
  public void close() {
    for (SharedInformerFactory factory : informerFactories) {
      try {
        factory.stopAllRegisteredInformers();
      } catch (Throwable ex) {
        logger.warn("Failed in stopAllRegisteredInformers", ex);
      }
    }

    for (KubernetesClient client : clients) {
      KubernetesHelper.closeQuietly(client);
    }
  }

  /**
   * Update DB with a new resource object, including job status, metadata and applicationId. Using
   * the submissionId hashset to reduce unnecessary updates.
   *
   * @param sparkCluster Spark cluster
   * @param prevCRDState the old resource object
   * @param currCRDState the new resource object
   */
  private void onUpdateImpl_logApplication(
      AppConfig.SparkCluster sparkCluster,
      SparkApplicationResource prevCRDState,
      SparkApplicationResource currCRDState) {
    String submissionId = currCRDState.getMetadata().getName();
    String newState = SparkApplicationResourceHelper.getState(currCRDState);
    String oldState = SparkApplicationResourceHelper.getState(prevCRDState);
    int driverCore = 0;
    int driverMemoryMb = 0;
    int executorInstances = 0;
    int executorCore = 0;
    int executorMemoryMb = 0;
    String dagName = "";
    String taskName = "";
    String appName = "";

    if (newState != null && !newState.equals(oldState)) {
      logger.info(
          "Got event for submission {}, state changed: {} -> {}", submissionId, oldState, newState);
      logDao.logApplicationStatus(submissionId, newState);

      Timestamp startTime = null;
      if (!driverInfoCollectedSubIds.contains(submissionId)
          && newState.equals(SparkConstants.RUNNING_STATE)) {
        try (DefaultKubernetesClient client = KubernetesHelper.getK8sClient(sparkCluster)) {
          String driverStartTime;
          SparkApplicationSpec spec = currCRDState.getSpec();
          DriverSpec driverSpec = null;
          ExecutorSpec executorSpec = null;
          Map<String, String> driverSparkConf = null;
          Map<String, String> driverSparkConfLabels = null;

          if (spec != null) {
            driverSpec = spec.getDriver();
            executorSpec = spec.getExecutor();
            driverSparkConf = spec.getSparkConf();
            if (driverSpec != null) {
              driverSparkConfLabels = driverSpec.getLabels();
              if (driverSparkConfLabels != null) {
                dagName = driverSparkConfLabels.get("dag_name");
                taskName = driverSparkConfLabels.get("task_name");
              }
            }
            if (driverSparkConf != null) {
              appName =
                  currCRDState.getSpec().getSparkConf().get(SparkConstants.SPARK_APP_NAME_LABEL);
            }
            if (executorSpec != null) {
              executorInstances = executorSpec.getInstances();
            }
          }

          Pod driver_pod;
          try {
            driver_pod =
                client
                    .pods()
                    .inNamespace(sparkCluster.getSparkApplicationNamespace())
                    .withName(submissionId + "-driver")
                    .get();
            if (driver_pod != null) {
              try {
                driverStartTime = driver_pod.getStatus().getStartTime();
                driverCore =
                    Integer.parseInt(
                        driver_pod
                            .getSpec()
                            .getContainers()
                            .get(0)
                            .getResources()
                            .getRequests()
                            .get("cpu")
                            .getAmount());
                driverMemoryMb =
                    Integer.valueOf(
                        driver_pod
                            .getSpec()
                            .getContainers()
                            .get(0)
                            .getResources()
                            .getRequests()
                            .get("memory")
                            .getAmount());
                if (StringUtils.isNotEmpty(driverStartTime)
                    && driverCore != 0
                    && driverMemoryMb != 0
                    && executorInstances != 0) {
                  startTime = new Timestamp(DateTimeUtils.parseOrNull(driverStartTime));
                  // only add the submission_id into the hashset when the info is successfully
                  // updated into database
                  if (logDao.updateJobInfo(
                      submissionId,
                      startTime,
                      driverCore,
                      driverMemoryMb,
                      executorInstances,
                      dagName,
                      taskName,
                      appName)) {
                    driverInfoCollectedSubIds.add(submissionId);
                  }
                }
              } catch (Exception e) {
                logger.warn(
                    String.format(
                        "Failed to get driver info from driver pod for submissionId %s",
                        submissionId),
                    e);
              }
            }
          } catch (Exception e) {
            logger.warn(
                String.format("Failed to get driver pod for submissionId %s", submissionId), e);
          }
        } catch (Exception e) {
          logger.warn(
              String.format("Failed to update driver info for submissionId %s", submissionId), e);
        }
      }

      if (currCRDState.getStatus() != null
          && currCRDState.getStatus().getSparkApplicationId() != null) {
        String appId = currCRDState.getStatus().getSparkApplicationId();
        logger.info("Got event for submission {}, application id: {}", submissionId, appId);
        logDao.logApplicationId(submissionId, appId);
      }
      if (SparkConstants.isApplicationStopped(newState)) {
        Timestamp finishedTime = null;
        if (currCRDState.getStatus() != null
            && currCRDState.getStatus().getTerminationTime() != null) {
          Long millis = DateTimeUtils.parseOrNull(currCRDState.getStatus().getTerminationTime());
          finishedTime = new Timestamp(millis);
        }
        logDao.logApplicationFinished(
            submissionId,
            newState,
            finishedTime,
            appConfig.getvCoreSecondCost(),
            appConfig.getMemoryMbSecondCost());
        driverInfoCollectedSubIds.remove(submissionId);
        String user = currCRDState.getMetadata().getLabels().get(Constants.PROXY_USER_LABEL);
        if (user == null) {
          user = "";
        }
        String sparkVersion = currCRDState.getSpec().getSparkVersion();
        if (sparkVersion == null) {
          sparkVersion = "";
        }
        requestCounters.increment(
            APPLICATION_FINISH_METRIC_NAME,
            Tag.of("status", newState),
            Tag.of("user", user),
            Tag.of("spark_version", sparkVersion),
            Tag.of(
                "spark_cluster",
                ApplicationSubmissionHelper.getClusterIdFromSubmissionId(submissionId)));
      }
    }
  }
}
