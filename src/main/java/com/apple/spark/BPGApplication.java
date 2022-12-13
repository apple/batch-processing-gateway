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

import static com.apple.spark.core.Constants.QUEUE_INFO;
import static com.apple.spark.core.Constants.SERVICE_ABBR;

import com.apple.spark.core.ApplicationMonitor;
import com.apple.spark.core.BPGStatsdConfig;
import com.apple.spark.core.Constants;
import com.apple.spark.core.ThrowableExceptionMapper;
import com.apple.spark.health.BPGHealthCheck;
import com.apple.spark.rest.AdminRest;
import com.apple.spark.rest.ApplicationGetLogRest;
import com.apple.spark.rest.ApplicationSubmissionRest;
import com.apple.spark.rest.HealthcheckRest;
import com.apple.spark.rest.S3Rest;
import com.apple.spark.security.User;
import com.apple.spark.security.UserNameAuthFilter;
import com.apple.spark.security.UserNameBasicAuthenticator;
import com.apple.spark.security.UserUnauthorizedHandler;
import com.apple.spark.util.CounterMetricContainer;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.Application;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.auth.chained.ChainedAuthFilter;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.swagger.v3.oas.integration.SwaggerConfiguration;
import io.swagger.v3.oas.models.OpenAPI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BPGApplication extends Application<AppConfig> {

  private static final Logger logger = LoggerFactory.getLogger(BPGApplication.class);

  private static final String PRINT_CONFIG_SYSTEM_PROPERTY_NAME = "printConfig"; // note: may leak credential information when print config
  private static final String MONITOR_APPLICATION_SYSTEM_PROPERTY_NAME = "monitorApplication";

  private final boolean monitorApplication;

  public BPGApplication() {
    this(false);
  }

  public BPGApplication(boolean monitorApplication) {
    this.monitorApplication = monitorApplication;
  }

  public static void main(final String[] args) throws Exception {
    logger.info(
        "Starting server, version: {}, revision: {}", BuildInfo.Version, BuildInfo.Revision);

    String value = System.getProperty(MONITOR_APPLICATION_SYSTEM_PROPERTY_NAME);
    boolean monitorApplication = value != null && value.equalsIgnoreCase("true");

    new BPGApplication(monitorApplication).run(args);
  }

  @Override
  public String getName() {
    return SERVICE_ABBR;
  }

  @Override
  public void initialize(final Bootstrap<AppConfig> bootstrap) {}

  @Override
  public void run(final AppConfig configuration, final Environment environment) {

    String value = System.getProperty(PRINT_CONFIG_SYSTEM_PROPERTY_NAME);
    boolean printConfig = value != null && value.equalsIgnoreCase("true");
    if (printConfig) {
      try {
        String str = new ObjectMapper().writeValueAsString(configuration);
        logger.info("Application configuration: {}", str);
      } catch (JsonProcessingException e) {
        logger.warn("Failed to serialize and print configuration", e);
      }
    }

    // Get the application's metric registry
    MetricRegistry registry = environment.metrics();
    SharedMetricRegistries.add(Constants.DEFAULT_METRIC_REGISTRY, registry);

    MeterRegistry meterRegistry = BPGStatsdConfig.createMeterRegistry();

    // Register all resources
    final ApplicationSubmissionRest applicationSubmissionRest =
        new ApplicationSubmissionRest(configuration, meterRegistry);
    environment.jersey().register(applicationSubmissionRest);

    final ApplicationGetLogRest applicationGetLogRest =
        new ApplicationGetLogRest(configuration, meterRegistry);
    environment.jersey().register(applicationGetLogRest);

    final S3Rest s3Rest = new S3Rest(configuration, meterRegistry);
    environment.jersey().register(s3Rest);

    final AdminRest adminRest = new AdminRest(configuration, meterRegistry);
    environment.jersey().register(adminRest);

    final HealthcheckRest healthcheckRest = new HealthcheckRest(configuration, meterRegistry);
    environment.jersey().register(healthcheckRest);

    environment.jersey().register(new ThrowableExceptionMapper(environment.metrics()));

    // To accept a username header
    UserNameAuthFilter<User> userNameAuthFilter =
        new UserNameAuthFilter.Builder<User>()
            .setAuthenticator(
                new UserNameBasicAuthenticator(
                    configuration.getAllowedUsers(), configuration.getBlockedUsers()))
            .setRealm(Constants.REALM)
            .setUnauthorizedHandler(new UserUnauthorizedHandler())
            .buildAuthFilter();

    // To accept basic authentication
    BasicCredentialAuthFilter<User> basicCredentialAuthFilter =
        new BasicCredentialAuthFilter.Builder<User>()
            .setAuthenticator(
                new UserNameBasicAuthenticator(
                    configuration.getAllowedUsers(), configuration.getBlockedUsers()))
            .setRealm(Constants.REALM)
            .setUnauthorizedHandler(new UserUnauthorizedHandler())
            .buildAuthFilter();

    // The auth will pass if any of the auth filters pass
    ChainedAuthFilter<BasicCredentials, User> chainedAuthFilter =
        new ChainedAuthFilter<>(Arrays.asList(userNameAuthFilter, basicCredentialAuthFilter));

    environment.jersey().register(new AuthDynamicFeature(chainedAuthFilter));

    // To use @Auth to inject a User Principal type into REST resource
    environment.jersey().register(new AuthValueFactoryProvider.Binder<>(User.class));

    final BPGHealthCheck healthCheck = new BPGHealthCheck(configuration.getSparkClusters());
    environment.healthChecks().register("sparkClusters", healthCheck);

    // Support OpenAPI spec for all components under com.apple.spark.rest
    OpenAPI openAPI = new OpenAPI();
    SwaggerConfiguration oasConfig =
        new SwaggerConfiguration()
            .openAPI(openAPI)
            .prettyPrint(true)
            .resourcePackages(new HashSet<>(Arrays.asList("com.apple.spark.rest")));

    environment.jersey().register(new OpenApiResource().openApiConfiguration(oasConfig));

    // This code block is for enabling CORS so that they can be reachable from
    // different domain/port
    // Opening this up can be risky but leaving the commented code here in case we are able to
    // enable it after evaluating the risks
    /*
    final FilterRegistration.Dynamic cors =
            environment.servlets().addFilter("CORS", CrossOriginFilter.class);

    cors.setInitParameter("allowedOrigins", "*");
    cors.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin");
    cors.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD");

    cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
     */

    if (monitorApplication) {
      logger.info("Starting application monitor");
      new ApplicationMonitor(configuration, meterRegistry).start();
    }

    sendPeriodicMetrics(configuration, meterRegistry);
  }

  private void sendPeriodicMetrics(AppConfig configuration, MeterRegistry meterRegistry) {
    CounterMetricContainer periodicMetrics = new CounterMetricContainer(meterRegistry);
    long metricInterval = 30000;
    new Timer(true)
        .schedule(
            new TimerTask() {
              @Override
              public void run() {
                try {
                  sendQueueInfoMetrics(configuration, periodicMetrics);
                } catch (Throwable ex) {
                  logger.warn("Failed to send queue info metrics", ex);
                }
              }
            },
            metricInterval,
            metricInterval);
  }

  /**
   * Go through clusters and send out metrics, so we could show which queues are on which clusters.
   *
   * @param configuration App configuration
   * @param periodicMetrics Metric instances
   */
  private void sendQueueInfoMetrics(
      AppConfig configuration, CounterMetricContainer periodicMetrics) {

    if (configuration.getSparkClusters() != null) {
      for (AppConfig.SparkCluster cluster : configuration.getSparkClusters()) {
        List<String> sparkVersions =
            cluster.getSparkVersions() == null ? new ArrayList<>() : cluster.getSparkVersions();
        List<String> queues = cluster.getQueues() == null ? new ArrayList<>() : cluster.getQueues();
        for (String sparkVersion : sparkVersions) {
          for (String queue : queues) {
            periodicMetrics.increment(
                QUEUE_INFO,
                Tag.of("eks", cluster.getEksCluster()),
                Tag.of("spark_cluster_id", cluster.getId()),
                Tag.of("spark_version", sparkVersion),
                Tag.of("queue", queue));
          }
        }
      }
    }
  }
}
