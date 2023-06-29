package com.apple.spark.appleinternal;

import static com.apple.spark.core.BatchSchedulerConstants.YUNIKORN_ROOT_QUEUE;
import static com.apple.spark.core.Constants.*;
import static com.apple.spark.core.Constants.AIML_OBSV_CUSTOM_TAG_LABEL;

import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.core.KubernetesHelper;
import com.apple.spark.crd.costattrib.CostAttributionSpec;
import com.apple.spark.crd.costattrib.CostAttributionTag;
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
      String submissionId,
      CostAttributionSpec costAttribution) {

    /* Apple AIML Spark team is developing new cost pipeline by CET team,
     so we need add new unified label to the pod by reusing pod info: queue name and proxy username
    */

    if (costAttribution == null || costAttribution.getTags() == null) {
      addLabel(sparkApplication, AIML_OBSV_USER_LABEL, proxyUser, true);
      addLabel(sparkApplication, AIML_OBSV_QUEUE_LABEL, YUNIKORN_ROOT_QUEUE + "." + queue, true);
      addLabel(sparkApplication, AIML_OBSV_JOB_ID_LABEL, submissionId, true);
      addLabel(sparkApplication, AIML_OBSV_JOB_NAME_LABEL, request.getApplicationName(), false);
      addLabel(sparkApplication, AIML_OBSV_PROJECT_ID_LABEL, request.getProjectId(), false);
      addLabel(sparkApplication, AIML_OBSV_PHASE_LABEL, request.getProjectPhase(), false);
      addLabel(sparkApplication, AIML_OBSV_CUSTOM_TAG_LABEL, request.getCustomTag(), false);
    } else {
      for (CostAttributionTag tag : costAttribution.getTags()) {
        switch (tag.getName()) {
          case AIML_OBSV_USER_LABEL -> addLabel(
              sparkApplication, AIML_OBSV_USER_LABEL, proxyUser, tag.getRequired());
          case AIML_OBSV_QUEUE_LABEL -> addLabel(
              sparkApplication,
              AIML_OBSV_QUEUE_LABEL,
              YUNIKORN_ROOT_QUEUE + "." + queue,
              tag.getRequired());
          case AIML_OBSV_JOB_ID_LABEL -> addLabel(
              sparkApplication, AIML_OBSV_JOB_ID_LABEL, submissionId, tag.getRequired());
          case AIML_OBSV_JOB_NAME_LABEL -> addLabel(
              sparkApplication,
              AIML_OBSV_JOB_NAME_LABEL,
              request.getApplicationName(),
              tag.getRequired());
          case AIML_OBSV_PROJECT_ID_LABEL -> addLabel(
              sparkApplication,
              AIML_OBSV_PROJECT_ID_LABEL,
              request.getProjectId(),
              tag.getRequired());
          case AIML_OBSV_PHASE_LABEL -> addLabel(
              sparkApplication,
              AIML_OBSV_PHASE_LABEL,
              request.getProjectPhase(),
              tag.getRequired());
          case AIML_OBSV_CUSTOM_TAG_LABEL -> addLabel(
              sparkApplication,
              AIML_OBSV_CUSTOM_TAG_LABEL,
              request.getCustomTag(),
              tag.getRequired());
        }
      }
    }
  }

  private static void addLabel(
      SparkApplication sparkApplication, String labelKey, String labelValue, boolean isRequired) {
    if (labelValue != null && !labelValue.isEmpty()) {
      String labelValueNormalized = KubernetesHelper.normalizeLabelValue(labelValue);
      sparkApplication.getMetadata().getLabels().put(labelKey, labelValueNormalized);
    } else {
      if (isRequired) {
        // todo reject the job directly when ready
        logger.error("COST LABELS MISSING: Cost attribution value of {} is empty", labelKey);
      } else {
        logger.warn("COST LABELS MISSING: Cost attribution value of {} is empty", labelKey);
      }
    }
  }
}
