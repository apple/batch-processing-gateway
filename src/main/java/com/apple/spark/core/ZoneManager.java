package com.apple.spark.core;

import com.apple.spark.AppConfig;
import com.apple.spark.AppConfig.QueueConfig;
import com.apple.spark.api.SubmitApplicationRequest;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZoneManager {
  private static final Logger logger = LoggerFactory.getLogger(ZoneManager.class);
  public static final String ROUND_ROBIN_ZONE_PICKER_NAME = "round_robin";
  private static final ConcurrentMap<String, ZonePicker> zonePickers = new ConcurrentHashMap<>();

  public static List<String> pickZones(
      final SubmitApplicationRequest request,
      final AppConfig appConfig,
      final String queueName,
      final AppConfig.SparkCluster sparkCluster,
      final String submissionId) {

    if (request == null || appConfig == null) {
      return null;
    }

    Optional<QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queueName)).findFirst();

    if (queueConfigOptional.isEmpty()) {
      return null;
    }

    QueueConfig queueConfig = queueConfigOptional.get();

    List<String> allowedZone = queueConfig.getAllowedZones();

    if (allowedZone == null || allowedZone.isEmpty()) {
      return null;
    }

    List<String> pickedZones =
        zonePickers
            .computeIfAbsent(queueConfig.getName(), (name) -> createZonePickerForQueue(queueConfig))
            .pick();

    logger.info(
        "Picked Zones: {} for job: {} in Queue: {}",
        String.join(",", pickedZones),
        submissionId,
        queueConfig.getName());

    return pickedZones;
  }

  private static ZonePicker createZonePickerForQueue(final QueueConfig queueConfig) {
    String zonePickerName = queueConfig.getZonePickerName();
    if (zonePickerName == null) {
      zonePickerName = ROUND_ROBIN_ZONE_PICKER_NAME;
    }

    ZonePicker zonePicker;
    switch (zonePickerName) {
      case ROUND_ROBIN_ZONE_PICKER_NAME:
        zonePicker = new RoundRobinZonePicker();
        break;
      default:
        zonePicker = new RoundRobinZonePicker();
    }

    zonePicker.update(queueConfig.getAllowedZones());
    logger.info(
        "Creating {} ZonePicker for Queue {} with AllowedAvailabilityZones {}",
        zonePickerName,
        queueConfig.getName(),
        String.join(",", queueConfig.getAllowedZones()));
    return zonePicker;
  }
}
