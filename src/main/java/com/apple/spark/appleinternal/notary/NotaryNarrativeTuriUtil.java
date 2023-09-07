package com.apple.spark.appleinternal.notary;

import com.apple.spark.AppConfig;
import com.apple.spark.operator.DriverSpec;
import com.apple.spark.operator.EnvVar;
import com.apple.spark.operator.ExecutorSpec;
import com.apple.spark.operator.SparkApplicationSpec;
import com.apple.spark.operator.Volume;
import com.apple.spark.operator.VolumeMount;
import io.fabric8.kubernetes.api.model.CSIVolumeSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotaryNarrativeTuriUtil {
  private static final Logger logger = LoggerFactory.getLogger(NotaryNarrativeTuriUtil.class);

  /**
   * Provision a turi domain Narrative identity volume for EKS pods.
   *
   * @param sparkSpec
   * @param config
   * @param personId
   */
  public static void addNotaryNarrativeTuriVolume(
      @NotNull SparkApplicationSpec sparkSpec, AppConfig config, String personId) {
    // Set up turi domain Narrative identity volume
    HashMap<String, String> csiVolumeAttributes = new HashMap<>();
    // the Turi narrative certificate the pod wants to get
    csiVolumeAttributes.put("narrative.apple.com/domain", "turi");
    csiVolumeAttributes.put("narrative.apple.com/name-json-person_id", personId);
    AppConfig.NotaryAppConfig notary = config.getNotary();
    String appNameSpace = notary.getAppNameSpace();
    csiVolumeAttributes.put("narrative.apple.com/name-json-aprn_namespace", appNameSpace);
    // the certificate is attested by the pod identity
    csiVolumeAttributes.put("narrative.apple.com/identity-attestor", "$pod");
    // the pod identity
    String narrativeTuriPodDomain = notary.getNarrativeTuriPodDomain();
    csiVolumeAttributes.put("narrative.apple.com/pod.domain", narrativeTuriPodDomain);
    logger.debug("Narrative pod domain: {}", narrativeTuriPodDomain);
    csiVolumeAttributes.put("narrative.apple.com/pod.pod-identity", "true");
    csiVolumeAttributes.put("narrative.apple.com/issuer-name", "spark-narrative-issuer");

    CSIVolumeSource csiVolume =
        new CSIVolumeSource("narrative.apple.com", null, null, true, csiVolumeAttributes);
    Volume notaryNarrativeTuriVolume = new Volume("turi-identity", csiVolume);

    // add volume to sparkSpec
    if (sparkSpec.getVolumes() != null) {
      sparkSpec.getVolumes().add(notaryNarrativeTuriVolume);
    } else {
      List<Volume> addedVolumes = new ArrayList<>();
      addedVolumes.add(notaryNarrativeTuriVolume);
      sparkSpec.setVolumes(addedVolumes);
    }

    // Add volumeMounts to driver and executor pods
    VolumeMount notaryNarrativeTuri = new VolumeMount("turi-identity", "/turi-identity");
    //  Add to driver pod
    if (sparkSpec.getDriver() == null) {
      sparkSpec.setDriver(new DriverSpec());
    }
    if (sparkSpec.getDriver().getVolumeMounts() == null) {
      List<VolumeMount> volumeMounts = new ArrayList<>();
      volumeMounts.add(notaryNarrativeTuri);
      sparkSpec.getDriver().setVolumeMounts(volumeMounts);
    } else {
      sparkSpec.getDriver().getVolumeMounts().add(notaryNarrativeTuri);
    }

    // Add to executor pod
    if (sparkSpec.getExecutor() == null) {
      sparkSpec.setExecutor(new ExecutorSpec());
    }
    if (sparkSpec.getExecutor().getVolumeMounts() == null) {
      List<VolumeMount> volumeMounts = new ArrayList<>();
      volumeMounts.add(notaryNarrativeTuri);
      sparkSpec.getExecutor().setVolumeMounts(volumeMounts);
    } else {
      sparkSpec.getExecutor().getVolumeMounts().add(notaryNarrativeTuri);
    }
  }

  /**
   * Set up the dsid annotations for EKS pod
   *
   * @param sparkSpec
   * @param personId
   */
  public static void addNotaryNarrativeTuriAnnotation(
      @NotNull SparkApplicationSpec sparkSpec, String personId) {
    // set the personId of the proxyUser as the annotations of the driver pod
    if (sparkSpec.getDriver() == null) {
      sparkSpec.setDriver(new DriverSpec());
    }
    DriverSpec driverSpec = sparkSpec.getDriver();
    Map<String, String> driverAnnotations =
        driverSpec.getAnnotations() == null ? new HashMap<>() : driverSpec.getAnnotations();
    driverAnnotations.put(NotaryConstants.NARRATIVE_TURI_ANNO_PERSON_ID_KEY, personId);
    driverSpec.setAnnotations(driverAnnotations);
    sparkSpec.setDriver(driverSpec);
    // set the personId of the proxyUser as the annotations of the executor pod
    if (sparkSpec.getExecutor() == null) {
      sparkSpec.setExecutor(new ExecutorSpec());
    }
    ExecutorSpec executorSpec = sparkSpec.getExecutor();
    Map<String, String> executorAnnotations =
        executorSpec.getAnnotations() == null ? new HashMap<>() : executorSpec.getAnnotations();
    executorAnnotations.put(NotaryConstants.NARRATIVE_TURI_ANNO_PERSON_ID_KEY, personId);
    executorSpec.setAnnotations(executorAnnotations);
    sparkSpec.setExecutor(executorSpec);
  }

  /**
   * Set mTLS certificates and key location as env var of EKS pods.
   *
   * @param sparkSpec
   */
  public static void addNotaryNarrativeEnvVar(@NotNull SparkApplicationSpec sparkSpec) {
    // set up the mtls crt and key location as ENV var
    EnvVar mtlsCrtEnvVar =
        new EnvVar(NotaryConstants.MTLS_CRT_LOCATION_KEY, NotaryConstants.MTLS_CRT_LOCATION_VAL);
    EnvVar mtlsKeyEnvVar =
        new EnvVar(NotaryConstants.MTLS_KEY_LOCATION_KEY, NotaryConstants.MTLS_KEY_LOCATION_VAL);

    // set mtls crt and key location as Env var of driver pods
    if (sparkSpec.getDriver() == null) {
      sparkSpec.setDriver(new DriverSpec());
    }
    if (sparkSpec.getDriver().getEnv() == null) {
      sparkSpec.getDriver().setEnv(new ArrayList<EnvVar>());
    }
    sparkSpec.getDriver().getEnv().add(mtlsCrtEnvVar);
    sparkSpec.getDriver().getEnv().add(mtlsKeyEnvVar);

    // set mtls crt and key location as Env var of executor pods
    if (sparkSpec.getExecutor() == null) {
      sparkSpec.setExecutor(new ExecutorSpec());
    }
    if (sparkSpec.getExecutor().getEnv() == null) {
      sparkSpec.getExecutor().setEnv(new ArrayList<EnvVar>());
    }
    sparkSpec.getExecutor().getEnv().add(mtlsCrtEnvVar);
    sparkSpec.getExecutor().getEnv().add(mtlsKeyEnvVar);
  }
}
