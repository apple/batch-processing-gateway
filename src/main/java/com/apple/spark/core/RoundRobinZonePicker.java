package com.apple.spark.core;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinZonePicker implements ZonePicker {

  private List<String> allowedZones;
  private final AtomicInteger index = new AtomicInteger(0);

  public RoundRobinZonePicker() {}

  @Override
  public void update(final List<String> allowedZones) {
    if (allowedZones == null || allowedZones.isEmpty()) {
      throw new RuntimeException("Invalid allowedZones: Empty");
    }
    if (allowedZones.stream().anyMatch(Objects::isNull)
        || allowedZones.stream().anyMatch(String::isEmpty)) {
      throw new RuntimeException("Invalid allowedZones: Some zone is empty");
    }
    this.allowedZones = allowedZones;
    this.index.set(0);
  }

  @Override
  public List<String> pick() {
    int zoneIndex = index.getAndUpdate((input) -> (input + 1) % allowedZones.size());
    String pickedZone = allowedZones.get(zoneIndex);
    return Collections.singletonList(pickedZone);
  }
}
