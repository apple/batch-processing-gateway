package com.apple.spark.appleinternal;

import static com.apple.spark.core.BatchSchedulerConstants.YUNIKORN_ROOT_QUEUE;
import static com.apple.spark.core.Constants.*;
import static com.apple.spark.core.Constants.AIML_OBSV_CUSTOM_TAG_LABEL;

import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.operator.SparkApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppleCostAttrUtils {

  private static final Logger logger = LoggerFactory.getLogger(AppleCostAttrUtils.class);

  public static void AddCostAttrLabelsToPods(
      SparkApplication sparkApplication,
      SubmitApplicationRequest request,
      String proxyUser,
      String queue,
      String submissionId) {

    /* Apple AIML Spark team is developing new cost pipeline by CET team,
     so we need add new unified label to the pod by reusing pod info: queue name and proxy username
    */
    addLabel(sparkApplication, AIML_OBSV_USER_LABEL, proxyUser);
    addLabel(sparkApplication, AIML_OBSV_QUEUE_LABEL, YUNIKORN_ROOT_QUEUE + "." + queue);
    addLabel(sparkApplication, AIML_OBSV_JOB_ID_LABEL, submissionId);
    addLabel(sparkApplication, AIML_OBSV_PROJECT_ID_LABEL, request.getProjectId());
    addLabel(sparkApplication, AIML_OBSV_PHASE_LABEL, request.getProjectPhase());
    addLabel(sparkApplication, AIML_OBSV_CUSTOM_TAG_LABEL, request.getCustomTag());
  }

  private static void addLabel(
      SparkApplication sparkApplication, String labelKey, String labelValue) {
    if (labelValue != null && !labelValue.isEmpty()) {
      sparkApplication.getMetadata().getLabels().put(labelKey, labelValue);
    } else {
      logger.error("ERROR COST LABELS MISSING: Cost attribution value of {} is empty", labelKey);
    }
  }
}
