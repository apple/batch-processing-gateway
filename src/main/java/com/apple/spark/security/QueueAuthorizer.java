package com.apple.spark.security;

import static com.apple.spark.core.Constants.QUEUE_LABEL;
import static com.apple.spark.core.Constants.SERVICE_ABBR;
import static io.micrometer.core.aop.TimedAspect.EXCEPTION_TAG;

import com.apple.spark.AppConfig;
import com.apple.spark.ranger.client.QueueAccessTypeAndUser;
import com.apple.spark.ranger.client.RangerSparkQueueClient;
import com.apple.spark.util.CounterMetricContainer;
import com.apple.spark.util.TimerMetricContainer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueAuthorizer {

  private static final Logger logger = LoggerFactory.getLogger(QueueAuthorizer.class);

  private final RangerSparkQueueClient rangerSparkQueueClient;

  private final Map<String, AppConfig.QueueConfig> queueConfigs;

  private final CounterMetricContainer failureMetrics;
  private final TimerMetricContainer timerMetrics;

  private static final String FAILURE_METRIC_NAME =
      String.format("statsd.%s.%s.authorizer.failure", SERVICE_ABBR, QUEUE_LABEL);
  private static final String TIMER_NAME =
      String.format("statsd.%s.%s.authorizer.timer", SERVICE_ABBR, QUEUE_LABEL);

  public QueueAuthorizer(
      MeterRegistry meterRegistry,
      Map<String, AppConfig.QueueConfig> queueConfigs,
      String policyRestUrl,
      String auditSolrUrl,
      Long userGroupsCacheDurationInMillis) {
    this.failureMetrics = new CounterMetricContainer(meterRegistry);
    this.timerMetrics = new TimerMetricContainer(meterRegistry);
    this.rangerSparkQueueClient =
        new RangerSparkQueueClient(policyRestUrl, auditSolrUrl, userGroupsCacheDurationInMillis);
    this.queueConfigs = queueConfigs;
  }

  /**
   * Return whether a queue is authorize enabled or not based on configuration.
   *
   * @param queue name
   * @return whether a queue is authorize enabled.
   */
  public boolean authorizeEnabled(String queue) {
    if (queue == null
        || !queueConfigs.containsKey(queue)
        || !queueConfigs.get(queue).getAuthorizeEnabled()) {
      return false;
    } else {
      return true;
    }
  }

  private List<Tag> generateMetricsTags(String queue, String accessType, String user) {
    return Arrays.asList(
        Tag.of(QUEUE_LABEL, queue), Tag.of("accessType", accessType), Tag.of("user", user));
  }

  public boolean isAuthorized(String queue, String accessType, String user) {
    return timerMetrics.record(
        () -> {
          try {
            return rangerSparkQueueClient.authorize(
                new QueueAccessTypeAndUser(queue, accessType, user));
          } catch (Exception ex) {
            logger.warn(
                String.format(
                    "Authorization failed. queue: %s, accessType: %s, user: %s",
                    queue, accessType, user),
                ex);

            List<Tag> metricsTagsList = generateMetricsTags(queue, accessType, user);
            metricsTagsList.add(Tag.of(EXCEPTION_TAG, ex.getClass().getSimpleName()));
            failureMetrics.increment(FAILURE_METRIC_NAME, metricsTagsList);

            return false;
          }
        },
        TIMER_NAME,
        generateMetricsTags(queue, accessType, user));
  }

  public void authorize(String queue, String accessType, String user) {
    if (!isAuthorized(queue, accessType, user)) {
      String unauthorizedLog =
          String.format(
              "Unauthorized on queue %s, access type %s, user %s", queue, accessType, user);
      logger.info(unauthorizedLog);
      throw new WebApplicationException(unauthorizedLog, Response.Status.FORBIDDEN);
    }
  }
}
