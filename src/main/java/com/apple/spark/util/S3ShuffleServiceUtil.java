package com.apple.spark.util;

import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.operator.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ShuffleServiceUtil {
  private static final Logger logger = LoggerFactory.getLogger(S3ShuffleServiceUtil.class);
  public static final String SPARK_SHUFFLE_MANAGER_KEY = "spark.shuffle.manager";

  public static final String SPARK_CLOUD_SHUFFLE_MANAGER_SHORT_VALUE = "csm";
  public static final String SPARK_CLOUD_SHUFFLE_MANAGER_FULL_VALUE =
      "org.apache.spark.shuffle.sort.DualShuffleManager";
  public static final String SPARK_CSM_EXTERNAL_PLUGIN_KEY = "spark.plugins";
  public static final String SPARK_CSM_EXTERNAL_PLUGIN_VALUE_BETA =
      "org.apache.spark.externalshuffle.sparkplugin.ExternalShuffleSparkPlugin";
  public static final String SPARK_CSM_EXTERNAL_PLUGIN_VALUE_GA =
      "org.apache.spark.csm.sparkplugin.ExternalShuffleSparkPlugin";
  public static final String SPARK_CSM_STORAGE_URT_KEY_BETA =
      "spark.shuffle.remote.storageMasterUri";
  public static final String SPARK_CSM_STORAGE_URT_KEY_GA = "spark.shuffle.csm.storageMasterUri";

  public S3ShuffleServiceUtil() {}

  /**
   * A spark job could specify to enable S3 based Shuffle Service to store shuffle data to S3
   * always, Users can still overwrite the conf when needed from request
   *
   * @param request
   * @param sparkSpec
   * @return
   */
  public static void applyS3BasedShuffleServiceSparkConf(
      SubmitApplicationRequest request, SparkApplicationSpec sparkSpec) {

    if (sparkSpec.getSparkConf() == null) {
      sparkSpec.setSparkConf(new HashMap<>());
    }

    if (request.getSparkConf() != null
        && request.getSparkConf().containsKey(SPARK_SHUFFLE_MANAGER_KEY)
        && request
            .getSparkConf()
            .get(SPARK_SHUFFLE_MANAGER_KEY)
            .equals(SPARK_CLOUD_SHUFFLE_MANAGER_FULL_VALUE)) { // beta release

      // add all default configs of dual shuffle manager
      final Map<String, String> dualShuffleManagerDefaultSparkConf = new HashMap<>();
      dualShuffleManagerDefaultSparkConf.put(
          "spark.plugins",
          "org.apache.spark.externalshuffle.sparkplugin.ExternalShuffleSparkPlugin");
      dualShuffleManagerDefaultSparkConf.put(
          "spark.shuffle.external.externalShufflePlugin",
          "org.apache.spark.externalshuffle.OAPShufflePlugin");
      dualShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.encryption.algorithm", "AES256");

      dualShuffleManagerDefaultSparkConf.put("spark.shuffle.remote.numReadThreads", "192");
      dualShuffleManagerDefaultSparkConf.put("spark.shuffle.remote.numIndexReadThreads", "192");
      dualShuffleManagerDefaultSparkConf.put("spark.hadoop.hadoop.tmp.dir", "/tmp/hadoop_tmp_dir");

      dualShuffleManagerDefaultSparkConf.put(
          "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      dualShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.block.size", "128M");
      dualShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.retry.throttle.interval", "50ms");

      dualShuffleManagerDefaultSparkConf.put(
          "spark.hadoop.fs.s3a.experimental.input.fadvise", "random");
      dualShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.threads.max", "192");
      dualShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.connection.max", "192");

      for (String confKey : dualShuffleManagerDefaultSparkConf.keySet()) {
        if (request.getSparkConf().containsKey(confKey)) {
          if (confKey.equals(SPARK_CSM_EXTERNAL_PLUGIN_KEY)) {
            String confVal = request.getSparkConf().get(confKey);
            if (confVal == null || confVal.isEmpty() || confVal.trim().isEmpty()) {
              sparkSpec.getSparkConf().put(confKey, SPARK_CSM_EXTERNAL_PLUGIN_VALUE_BETA);
            } else {
              sparkSpec
                  .getSparkConf()
                  .put(
                      confKey,
                      request.getSparkConf().get(confKey).trim()
                          + ","
                          + SPARK_CSM_EXTERNAL_PLUGIN_VALUE_BETA);
            }
          } else {
            sparkSpec.getSparkConf().put(confKey, request.getSparkConf().get(confKey));
          }
        } else { // set default configs if request dose not include the configs
          sparkSpec.getSparkConf().put(confKey, dualShuffleManagerDefaultSparkConf.get(confKey));
        }
      }

      /* Disable the decommission feature of vanilla Spark since there is conflict to update map output file status
        when enabling dual shuffle manager, we do not allow user to enable it when dual shuffle manager is used
      */
      sparkSpec.getSparkConf().put("spark.decommission.enabled", "false");
      sparkSpec.getSparkConf().put("spark.storage.decommission.enabled", "false");
      logger.info(
          String.format(
              "Decommission feature of vanilla Spark is disabled when enabling dual shuffle"
                  + " manager"));

      logger.info(
          String.format(
              "S3-based shuffle service enabled and the shuffle manager name is %s",
              SPARK_CLOUD_SHUFFLE_MANAGER_FULL_VALUE));
      // check storage uri
      if (!sparkSpec.getSparkConf().containsKey(SPARK_CSM_STORAGE_URT_KEY_BETA)) {
        logger.error(String.format(" %s is empty, please check", SPARK_CSM_STORAGE_URT_KEY_BETA));
      }

      if (sparkSpec.getSparkConf().get(SPARK_CSM_STORAGE_URT_KEY_BETA) == null) {
        logger.error(
            String.format("value of %s is empty, please check", SPARK_CSM_STORAGE_URT_KEY_BETA));
      }

    } else if (request.getSparkConf() != null
        && request.getSparkConf().containsKey(SPARK_SHUFFLE_MANAGER_KEY)
        && request
            .getSparkConf()
            .get(SPARK_SHUFFLE_MANAGER_KEY)
            .equals(SPARK_CLOUD_SHUFFLE_MANAGER_SHORT_VALUE)) { // SHORT NAME STANDS FOR GA RELEASE

      // add all default configs of dual shuffle manager
      final Map<String, String> csmShuffleManagerDefaultSparkConf = new HashMap<>();

      csmShuffleManagerDefaultSparkConf.put(
          "spark.plugins", "org.apache.spark.csm.sparkplugin.ExternalShuffleSparkPlugin");
      csmShuffleManagerDefaultSparkConf.put(
          "spark.shuffle.csm.externalShufflePlugin", "org.apache.spark.csm.OAPShufflePlugin");
      csmShuffleManagerDefaultSparkConf.put("spark.shuffle.csm.numReadThreads", "192");
      csmShuffleManagerDefaultSparkConf.put("spark.shuffle.csm.numIndexReadThreads", "192");

      csmShuffleManagerDefaultSparkConf.put("spark.hadoop.hadoop.tmp.dir", "/tmp/hadoop_tmp_dir");
      csmShuffleManagerDefaultSparkConf.put(
          "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

      //      csmShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.encryption.algorithm",
      // "AES256");

      //      csmShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.block.size", "128M");
      //      csmShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.retry.throttle.interval",
      // "50ms");

      //      csmShuffleManagerDefaultSparkConf.put(
      //          "spark.hadoop.fs.s3a.experimental.input.fadvise", "random");
      //      csmShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.threads.max", "192");
      //      csmShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.connection.max", "192");

      for (String confKey : csmShuffleManagerDefaultSparkConf.keySet()) {
        if (request.getSparkConf().containsKey(confKey)) {
          if (confKey.equals(SPARK_CSM_EXTERNAL_PLUGIN_KEY)) {
            String confVal = request.getSparkConf().get(confKey);
            if (confVal == null || confVal.isEmpty() || confVal.trim().isEmpty()) {
              sparkSpec.getSparkConf().put(confKey, SPARK_CSM_EXTERNAL_PLUGIN_VALUE_GA);
            } else {
              sparkSpec
                  .getSparkConf()
                  .put(
                      confKey,
                      request.getSparkConf().get(confKey).trim()
                          + ","
                          + SPARK_CSM_EXTERNAL_PLUGIN_VALUE_GA);
            }
          } else {
            sparkSpec.getSparkConf().put(confKey, request.getSparkConf().get(confKey));
          }
        } else { // set default configs if request dose not include the configs
          sparkSpec.getSparkConf().put(confKey, csmShuffleManagerDefaultSparkConf.get(confKey));
        }
      }

      // convert csm short name to full name
      sparkSpec
          .getSparkConf()
          .put(SPARK_SHUFFLE_MANAGER_KEY, SPARK_CLOUD_SHUFFLE_MANAGER_FULL_VALUE);

      // check storage uri
      if (!sparkSpec.getSparkConf().containsKey(SPARK_CSM_STORAGE_URT_KEY_GA)) {
        logger.error(String.format(" %s is empty, please check", SPARK_CSM_STORAGE_URT_KEY_GA));
      }

      if (sparkSpec.getSparkConf().get(SPARK_CSM_STORAGE_URT_KEY_GA) == null) {
        logger.error(
            String.format("value of %s is empty, please check", SPARK_CSM_STORAGE_URT_KEY_GA));
      }

      /* Disable the decommission feature of vanilla Spark since there is conflict to update map output file status
        when enabling dual shuffle manager, we do not allow user to enable it when dual shuffle manager is used
      */
      sparkSpec.getSparkConf().put("spark.decommission.enabled", "false");
      sparkSpec.getSparkConf().put("spark.storage.decommission.enabled", "false");
      logger.info(
          String.format(
              "Decommission feature of vanilla Spark is disabled when enabling dual shuffle"
                  + " manager"));

      logger.info(
          String.format(
              "S3-based shuffle service enabled and the shuffle manager name is %s",
              SPARK_CLOUD_SHUFFLE_MANAGER_FULL_VALUE));
    } else {
      logger.error("Shuffle Manager user specified is not found, please check !");
    }
  }
}
