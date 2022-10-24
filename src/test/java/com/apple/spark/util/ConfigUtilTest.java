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

package com.apple.spark.util;

import static com.apple.spark.AppConfig.SparkCluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConfigUtilTest {

  private final SparkCluster c1 = new SparkCluster();
  private final SparkCluster c2 = new SparkCluster();
  private final SparkCluster c3 = new SparkCluster();
  private final SparkCluster c4 = new SparkCluster();
  private final List<SparkCluster> clustersCM = new ArrayList<>();
  private final List<SparkCluster> clustersDB = new ArrayList<>();

  @Test
  public void merge() {
    // case 1: config map does not have spark cluster conf, all conf is in DB.
    clustersDB.addAll(Arrays.asList(c1, c2, c3));
    List<SparkCluster> combinedClusters = ConfigUtil.merge(null, clustersDB);
    Assert.assertEquals(combinedClusters.size(), 3);
    reset();

    // case 2: config map and DB both have spark cluster conf with no overlap.
    clustersCM.addAll(Arrays.asList(c1));
    clustersDB.addAll(Arrays.asList(c2, c3));
    combinedClusters = ConfigUtil.merge(clustersCM, clustersDB);
    Assert.assertEquals(combinedClusters.size(), 3);
    reset();

    // case 3: config map and DB both have spark cluser conf with overlap based on cid.
    clustersCM.addAll(Arrays.asList(c1, c2));
    clustersDB.addAll(Arrays.asList(c3, c4));
    combinedClusters = ConfigUtil.merge(clustersCM, clustersDB);
    Assert.assertEquals(combinedClusters.size(), 3);
    // for overlap, conf from config map wins.
    Assert.assertTrue(combinedClusters.stream().anyMatch(t -> t.getEksCluster().equals("eks02")));
    reset();

    // case 4: config map has spark cluster conf, DB does not.
    clustersCM.addAll(Arrays.asList(c1, c2, c3));
    combinedClusters = ConfigUtil.merge(clustersCM, clustersDB);
    Assert.assertEquals(combinedClusters.size(), 3);

    // case 5: config map or db has spark cluster conf, however, c2 and c4 overlap on cid - the one
    // that comes first wins.
    clustersCM.addAll(Arrays.asList(c1, c2, c3, c4));
    combinedClusters = ConfigUtil.merge(clustersCM, clustersDB);
    Assert.assertEquals(combinedClusters.size(), 3);
    reset();

    // case 6: Neither configmap nor DB has Spark cluster conf, expect a RuntimeException.
    Assert.assertThrows(RuntimeException.class, () -> ConfigUtil.merge(null, clustersDB));
  }

  @BeforeMethod
  public void beforeMethod() {
    c1.setId("c0201");
    c1.setEksCluster("eks01");

    c2.setId("c0202");
    c2.setEksCluster("eks02");

    c3.setId("c0301");
    c3.setEksCluster("eks03");

    c4.setId("c0202");
    c4.setEksCluster("eks01-other");
  }

  public void reset() {
    clustersCM.clear();
    clustersDB.clear();
  }

  @AfterMethod
  public void afterMethod() {}
}
