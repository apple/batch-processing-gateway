package com.apple.spark.operator;

import com.apple.spark.AppConfig;
import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.initContainer.JobInitDependencies;
import io.fabric8.kubernetes.api.model.*;
// import io.fabric8.kubernetes.api.model.Volume;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkJobInitDependenciesInitContainerUtil {

  private static final Logger logger =
      LoggerFactory.getLogger(SparkJobInitDependenciesInitContainerUtil.class);

  public static final String USER_DEPENDENCIES_INTI_CONTAINER_NAME =
      "user-dependencies-download-init-container";
  public static final String ENV_INIT_CONTAINER_USER_DEPENDENCIES_DIR = "DEPENDENCIES_PATH";
  public static final String ENV_INIT_CONTAINER_TERMINATION_MESSAGE_PATH =
      "TERMINATION_MESSAGE_PATH";
  public static final String DEFAULT_TERMINATION_MESSAGE_PATH = "/mnt/termination-log";
  public static final String DEFAULT_DEPENDENCIES_VOLUME_NAME = "dependencies";
  public static final String DEFAULT_DEPENDENCIES_VOLUME_PATH = "/mnt/init-dependencies";
  public static final String DEFAULT_INIT_CONTAINER_CORES_REQUEST_ON_K8S = "0.01";
  public static final String DEFAULT_INIT_CONTAINER_CORES_LIMIT_ON_K8S = "1";
  public static final String DEFAULT_INIT_CONTAINER_MEMORY_REQUEST_ON_K8S = "2048Mi";
  public static final String DEFAULT_INIT_CONTAINER_MEMORY_LIMIT_ON_K8S = "4096Mi";
  public static final String DEFAULT_INIT_CONTAINER_EPHEMERAL_STORAGE_REQUEST_ON_K8S = "1Gi";
  public static final String DEFAULT_INIT_CONTAINER_EPHEMERAL_STORAGE_LIMIT_ON_K8S = "10Gi";

  // todo make this configurable
  public static final String DEFAULT_USER_DEPENDENCIES_INIT_CONTAINER_IMAGE_URI =
      "docker.apple.com/aiml/di-batch/user-dependencies:0.1-2";
  public static final long DEFAULT_USER_DEPENDENCIES_INIT_CONTAINER_DEFAULT_USER_ID = 65534L;
  public static final long DEFAULT_USER_DEPENDENCIES_INIT_CONTAINER_DEFAULT_GROUP_ID = 65534L;

  public static void enableUserInitDependenciesSupport(
      SparkApplicationSpec sparkSpec, SubmitApplicationRequest request, AppConfig appConfig) {
    // If start an init container to download user dependencies
    if (enableBuiltInInitContainer(request)) {

      ResourceRequirementsBuilder resourceRequirementsBuilder = new ResourceRequirementsBuilder();
      resourceRequirementsBuilder
          .addToRequests("cpu", new Quantity(DEFAULT_INIT_CONTAINER_CORES_REQUEST_ON_K8S))
          .addToLimits("cpu", new Quantity(DEFAULT_INIT_CONTAINER_CORES_LIMIT_ON_K8S))
          .addToRequests("memory", new Quantity(DEFAULT_INIT_CONTAINER_MEMORY_REQUEST_ON_K8S))
          .addToLimits("memory", new Quantity(DEFAULT_INIT_CONTAINER_MEMORY_LIMIT_ON_K8S))
          .addToRequests(
              "ephemeral-storage",
              new Quantity(DEFAULT_INIT_CONTAINER_EPHEMERAL_STORAGE_REQUEST_ON_K8S))
          .addToLimits(
              "ephemeral-storage",
              new Quantity(DEFAULT_INIT_CONTAINER_EPHEMERAL_STORAGE_LIMIT_ON_K8S));

      JobInitDependencies dependencies = request.getJobInitDependencies();

      List<String> userDependenciesArgs = new ArrayList<>();
      userDependenciesArgs.add("/opt/download-dependencies.sh \"$0\"");
      userDependenciesArgs.add(dependencies.getUrls().stream().collect(Collectors.joining(" ")));
      userDependenciesArgs.add("2> >(tee -a $TERMINATION_MESSAGE_PATH >&2)");

      EmptyDirVolumeSource emptyDirVolumeSource = new EmptyDirVolumeSource();
      com.apple.spark.operator.Volume userDependenciesStoreVolume =
          new com.apple.spark.operator.Volume(
              DEFAULT_DEPENDENCIES_VOLUME_NAME, emptyDirVolumeSource);

      // add emptyDir volume to spark spec part
      if (sparkSpec.getVolumes() != null) {
        sparkSpec.getVolumes().add(userDependenciesStoreVolume);
      } else {
        List<Volume> addedVolumes = new ArrayList<>();
        addedVolumes.add(userDependenciesStoreVolume);
        sparkSpec.setVolumes(addedVolumes);
      }

      // add volume mounts of the new volume to driver
      VolumeMount jobDeps =
          new VolumeMount(DEFAULT_DEPENDENCIES_VOLUME_NAME, DEFAULT_DEPENDENCIES_VOLUME_PATH);
      if (sparkSpec.getDriver() != null && sparkSpec.getDriver().getVolumeMounts() != null) {
        sparkSpec.getDriver().getVolumeMounts().add(jobDeps);
      } else {
        List<VolumeMount> volumeMounts = new ArrayList<>();
        volumeMounts.add(jobDeps);
        sparkSpec.getDriver().setVolumeMounts(volumeMounts);
      }

      String myImageURI =
          appConfig.getJobDependenciesInitDockerImage() == null
              ? DEFAULT_USER_DEPENDENCIES_INIT_CONTAINER_IMAGE_URI
              : appConfig.getJobDependenciesInitDockerImage();

      ContainerBuilder userDependenciesContainerBuilder =
          new ContainerBuilder()
              .withName(USER_DEPENDENCIES_INTI_CONTAINER_NAME)
              .withImage(myImageURI)
              .withCommand("/bin/bash", "-c")
              .withArgs(userDependenciesArgs)
              .addNewEnv()
              .withName(ENV_INIT_CONTAINER_USER_DEPENDENCIES_DIR)
              .withValue(DEFAULT_DEPENDENCIES_VOLUME_PATH)
              .endEnv()
              .addNewEnv()
              .withName(ENV_INIT_CONTAINER_TERMINATION_MESSAGE_PATH)
              .withValue(DEFAULT_TERMINATION_MESSAGE_PATH)
              .endEnv()
              .addNewVolumeMount()
              .withName(DEFAULT_DEPENDENCIES_VOLUME_NAME)
              .withMountPath(DEFAULT_DEPENDENCIES_VOLUME_PATH)
              .endVolumeMount()
              .withTerminationMessagePath(DEFAULT_TERMINATION_MESSAGE_PATH)
              .withTerminationMessagePolicy("FallbackToLogsOnError")
              .withResources(resourceRequirementsBuilder.build())
              .withSecurityContext(getInitContainerSecurityContext().build());

      if (sparkSpec.getDriver() != null && sparkSpec.getDriver().getInitContainers() != null) {
        sparkSpec.getDriver().getInitContainers().add(userDependenciesContainerBuilder.build());
      } else {
        List<Container> stdInitContainers = new ArrayList<>();
        stdInitContainers.add(userDependenciesContainerBuilder.build());
        if (sparkSpec.getDriver() != null) {
          sparkSpec.getDriver().setInitContainers(stdInitContainers);
        }
      }

      // need to check later
      // sparkSpec.getExecutor().getSTDInitContainers().add(userDependenciesContainerBuilder.build());

    }
  }

  /**
   * A spark job could specify job's initial dependencies prior Spark JVM start
   *
   * @param request
   * @return enableInitContainer
   */
  public static boolean enableBuiltInInitContainer(SubmitApplicationRequest request) {
    return request.getJobInitDependencies() != null
        && request.getJobInitDependencies().getUrls() != null;
  }

  public static SecurityContextBuilder getInitContainerSecurityContext() {
    SecurityContextBuilder securityContextBuilder = new SecurityContextBuilder();
    securityContextBuilder.withRunAsNonRoot(true);
    securityContextBuilder.withAllowPrivilegeEscalation(false);
    securityContextBuilder.withRunAsUser(DEFAULT_USER_DEPENDENCIES_INIT_CONTAINER_DEFAULT_USER_ID);
    securityContextBuilder.withRunAsGroup(
        DEFAULT_USER_DEPENDENCIES_INIT_CONTAINER_DEFAULT_GROUP_ID);
    return securityContextBuilder;
  }
}
