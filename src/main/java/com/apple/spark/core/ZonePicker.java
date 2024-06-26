package com.apple.spark.core;

import java.util.List;

public interface ZonePicker {
  void update(List<String> allowedAvailabilityZones);

  // return a list of zone ids, order matters
  List<String> pick();
}
