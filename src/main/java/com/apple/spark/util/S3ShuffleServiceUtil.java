package com.apple.spark.util;

import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.operator.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ShuffleServiceUtil {
  private static final Logger logger = LoggerFactory.getLogger(S3ShuffleServiceUtil.class);
  public static final String APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_KEY = "spark.shuffle.manager";
  public static final String APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_OAP_VALUE =
      "org.apache.spark.shuffle.remote.RemoteShuffleManager";
  public static final String APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_DUAL_VALUE =
      "org.apache.spark.shuffle.sort.DualShuffleManager";
  public static final String APPLE_S3_SHUFFLE_SERVICE_EXTERNAL_PLUGIN_KEY = "spark.plugins";
  public static final String APPLE_S3_SHUFFLE_SERVICE_EXTERNAL_PLUGIN_VALUE =
      "org.apache.spark.externalshuffle.sparkplugin.ExternalShuffleSparkPlugin";

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

    if (request
        .getSparkConf()
        .get(APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_KEY)
        .equals(APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_OAP_VALUE)) {

      // add all default configs of oap shuffle manager,
      final Map<String, String> oapShuffleManagerDefaultSparkConf = new HashMap<>();

      // there is no fallback mechanism when mapper lost,  so we will disable index caching feature
      // by default
      // oapShuffleManagerDefaultSparkConf.put("spark.shuffle.remote.index.cache.size", "150m");

      oapShuffleManagerDefaultSparkConf.put("spark.shuffle.remote.numReadThreads", "192");
      oapShuffleManagerDefaultSparkConf.put("spark.shuffle.remote.numIndexReadThreads", "192");
      oapShuffleManagerDefaultSparkConf.put("spark.hadoop.hadoop.tmp.dir", "/tmp/hadoop_tmp_dir");

      oapShuffleManagerDefaultSparkConf.put(
          "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      oapShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.block.size", "128M");
      oapShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.retry.throttle.interval", "50ms");
      oapShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.fast.upload", "true");

      oapShuffleManagerDefaultSparkConf.put(
          "spark.hadoop.fs.s3a.experimental.input.fadvis", "random");
      oapShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.threads.max", "192");
      oapShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.connection.max", "192");

      // these are S3 related optimization configs which is not directly related to s3
      // shuffle service, just comment out for now
      //      oapShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.committer.name", "magic");
      //      oapShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.committer.magic.enabled",
      // "true");
      //      oapShuffleManagerDefaultSparkConf.put(
      //          "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
      //          "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory");
      //      oapShuffleManagerDefaultSparkConf.put(
      //          "spark.sql.parquet.output.committer.class",
      //          "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter");

      for (String confKey : oapShuffleManagerDefaultSparkConf.keySet()) {
        if (request.getSparkConf().containsKey(confKey)) {
          sparkSpec.getSparkConf().put(confKey, request.getSparkConf().get(confKey));
        } else {
          sparkSpec.getSparkConf().put(confKey, oapShuffleManagerDefaultSparkConf.get(confKey));
        }
      }
      logger.info(
          String.format(
              "S3-based shuffle service enabled and the shuffle manager name is %s",
              APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_OAP_VALUE));
    } else if (request
        .getSparkConf()
        .get(APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_KEY)
        .equals(APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_DUAL_VALUE)) {

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

      // these are S3 related optimization configs which is not directly related to s3
      // shuffle service, just comment out for now
      //      dualShuffleManagerDefaultSparkConf.put(
      //          "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
      //          "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory");
      //      dualShuffleManagerDefaultSparkConf.put(
      //          "spark.sql.parquet.output.committer.class",
      //          "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter");
      //      dualShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.fast.upload", "true");
      //      dualShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.committer.name", "magic");
      //      dualShuffleManagerDefaultSparkConf.put("spark.hadoop.fs.s3a.committer.magic.enabled",
      // "true");

      for (String confKey : dualShuffleManagerDefaultSparkConf.keySet()) {
        if (request.getSparkConf().containsKey(confKey)) {
          if (confKey.equals(APPLE_S3_SHUFFLE_SERVICE_EXTERNAL_PLUGIN_KEY)) {
            String confVal = request.getSparkConf().get(confKey);
            if (confVal == null || confVal.isEmpty() || confVal.trim().isEmpty()) {
              sparkSpec.getSparkConf().put(confKey, APPLE_S3_SHUFFLE_SERVICE_EXTERNAL_PLUGIN_VALUE);
            } else {
              sparkSpec
                  .getSparkConf()
                  .put(
                      confKey,
                      request.getSparkConf().get(confKey).trim()
                          + ","
                          + APPLE_S3_SHUFFLE_SERVICE_EXTERNAL_PLUGIN_VALUE);
            }
          } else {
            sparkSpec.getSparkConf().put(confKey, request.getSparkConf().get(confKey));
          }
        } else { // set default configs if request dose not include the configs
          sparkSpec.getSparkConf().put(confKey, dualShuffleManagerDefaultSparkConf.get(confKey));
        }
      }
      logger.info(
          String.format(
              "S3-based shuffle service enabled and the shuffle manager name is %s",
              APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_DUAL_VALUE));
    } else {
      logger.error("Shuffle Manager user specified is not found, please check !");
    }
  }

  public static boolean enableS3BasedShuffleService(SubmitApplicationRequest request) {
    if (request.getSparkConf() != null) {
      if (request.getSparkConf().containsKey(APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_KEY)) {
        if (request
                .getSparkConf()
                .get(APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_KEY)
                .equals(APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_OAP_VALUE)
            || request
                .getSparkConf()
                .get(APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_KEY)
                .equals(APPLE_S3_SHUFFLE_SERVICE_SHUFFLE_MANAGER_DUAL_VALUE)) {
          return true;
        } else {
          logger.error(
              String.format("Shuffle Manager user specified is not found, please check !"));
          return false;
        }
      }
    }
    return false;
  }
}
