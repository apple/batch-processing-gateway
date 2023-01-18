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

import com.apple.spark.AppConfig;
import com.apple.spark.core.DBConnection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUtil {

  private static final Logger logger = LoggerFactory.getLogger(ConfigUtil.class);

  public static void mergeConfig(AppConfig config) {
    List<SparkCluster> clustersFromDB = getConfFromDB(config);
    List<SparkCluster> clustersFromCM = config.getSparkClusters();
    List<SparkCluster> combinedSparkClusters = merge(clustersFromCM, clustersFromDB);
    config.setSparkClusters(combinedSparkClusters);
  }

  public static String getSparkUIUrl(SparkCluster cluster, String submissionId) {
    return String.format(
        "%s/%s/%s", cluster.getSparkUIUrl(), cluster.getSparkApplicationNamespace(), submissionId);
  }

  public static String getSparkHistoryUrl(String sparkHistoryDns, String appId) {
    return String.format("https://%s/history/%s", sparkHistoryDns, appId);
  }

  public static List<SparkCluster> merge(List<SparkCluster> c1, List<SparkCluster> c2) {
    Set<SparkCluster> combinedSet = new TreeSet<>(Comparator.comparing(SparkCluster::getId));

    if (c1 != null) {
      combinedSet.addAll(c1);
    }
    combinedSet.addAll(c2);

    List<SparkCluster> combinedSparkClusters = new ArrayList<>(combinedSet);

    if (combinedSparkClusters.isEmpty()) {
      throw new RuntimeException(
          "Could not get Spark Clusters configuration from DB and k8s configmap. Exit.");
    }

    return combinedSparkClusters;
  }

  public static List<SparkCluster> getConfFromDB(AppConfig config) {
    AppConfig.DBStorage dbconf = config.getDbStorageSOPS();
    String connectionString = dbconf.getConnectionString();
    String userId = dbconf.getUser();
    String password = dbconf.getPasswordDecodedValue();
    String queryConf = "SELECT cid, conf FROM config";

    DBConnection dbConnection = new DBConnection(connectionString, userId, password);
    List<SparkCluster> sparkClusters = new ArrayList<>();
    ObjectMapper objectMapper = new ObjectMapper();

    try {
      Jdbi jdbi = Jdbi.create(dbConnection.getConnection());
      List<Map<String, Object>> dbResults =
          jdbi.withHandle(handle -> handle.createQuery(queryConf).mapToMap().list());

      for (Map<String, Object> m : dbResults) {
        String conf = (String) m.get("conf");
        SparkCluster sparkCluster = objectMapper.readValue(conf, SparkCluster.class);
        if (sparkCluster != null) {
          sparkClusters.add(sparkCluster);
          logger.info(
              "Read configuration from db for spark cluster/namespace: {}", sparkCluster.getId());
        }
      }
    } catch (Exception e) {
      logger.info("Error occurred fetching and parsing config from DB: {}.", e.toString());
    }

    if (sparkClusters.isEmpty()) {
      logger.info("No spark cluster configuration was read from DB.");
    }

    return sparkClusters;
  }

  public static VersionInfo readVersion() {
    VersionInfo verInfo = new VersionInfo();
    try {
      InputStream in = ConfigUtil.class.getResourceAsStream("/version.txt");
      ObjectMapper mapper_yaml = new ObjectMapper(new YAMLFactory());
      verInfo = mapper_yaml.readValue(in, VersionInfo.class);
    } catch (Exception e) {
      logger.error("version.txt not found");
    }
    return verInfo;
  }
}
