package com.apple.spark.appleinternal;

import static com.apple.spark.core.Constants.*;

import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.core.KubernetesHelper;
import com.apple.spark.crd.costattrib.CostAttributionSpec;
import com.apple.spark.crd.costattrib.CostAttributionTag;
import com.apple.spark.operator.SparkApplication;
import java.util.List;
import java.util.Map;
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

    // The following 3 tags are system generated tags for cost pipeline, we will always support them
    addLabel(sparkApplication, AIML_OBSV_USER_LABEL, proxyUser, true);
    addLabel(sparkApplication, AIML_OBSV_QUEUE_LABEL, queue, true);
    addLabel(sparkApplication, AIML_OBSV_JOB_ID_LABEL, submissionId, true);

    // for other customer tags inject from request, we will
    if (costAttribution != null && costAttribution.getTags() != null) {
      List<CostAttributionTag> supportedTags = costAttribution.getTags();
      if (request.getCustomCostAttributionTags() != null
          && !request.getCustomCostAttributionTags().isEmpty()) {
        Map<String, String> input = request.getCustomCostAttributionTags();
        for (String tagKey : input.keySet()) {
          for (CostAttributionTag supportedTag : supportedTags) {
            if (tagKey.equals(supportedTag.getName())) {
              addLabel(sparkApplication, tagKey, input.get(tagKey), supportedTag.getRequired());
            } else {
              logger.warn(
                  "COST LABEL UNSUPPORTED : Cost attribution CRD dose not support the {} injected"
                      + " by customer",
                  tagKey);
            }
          }
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
        logger.error("COST LABEL MISSING: Cost attribution value of {} is empty", labelKey);
      } else {
        logger.warn("COST LABEL MISSING: Cost attribution value of {} is empty", labelKey);
      }
    }
  }
}
