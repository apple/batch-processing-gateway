package com.apple.spark.util;

import static org.mockito.Mockito.*;

import com.apple.spark.AppConfig;
import com.apple.spark.appleinternal.notary.NotaryConstants;
import com.apple.spark.appleinternal.notary.NotaryPersonIdUtil;
import com.apple.spark.operator.*;
import com.apple.spark.rest.ApplicationSubmissionRest;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.ws.rs.container.ContainerRequestContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

/**
 * Test class for NotaryEnableFlag. This class tests the behavior of the notary enabled flag in the
 * ApplicationSubmissionRest service. It covers scenarios where the flag is set to both true and
 * false and verifies the correct behavior. The tests use Mockito for mocking dependencies and
 * validate the processing logic.
 */
public class NotaryEnableFlagTest {

  @Mock private SparkApplicationSpec mockSparkSpec;
  @Mock private ContainerRequestContext mockRequestContext;
  @Mock private AppConfig mockAppConfig;
  @Mock private AppConfig.QueueConfig mockQueueConfig;
  @Mock private AppConfig.NotaryAppConfig mockNotaryAppConfig;

  @Mock private com.apple.spark.operator.DriverSpec mockDriverSpec;
  @Mock private com.apple.spark.operator.ExecutorSpec mockExecutorSpec;

  private ApplicationSubmissionRest applicationSubmissionRest;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Mock SparkApplicationSpec and its dependencies
    mockSparkSpec = mock(SparkApplicationSpec.class);
    mockDriverSpec = mock(DriverSpec.class);
    mockExecutorSpec = mock(ExecutorSpec.class);

    List<Volume> volumes = new ArrayList<>();
    List<VolumeMount> driverVolumeMounts = new ArrayList<>();
    List<VolumeMount> executorVolumeMounts = new ArrayList<>();

    when(mockSparkSpec.getVolumes()).thenReturn(volumes);
    when(mockSparkSpec.getDriver()).thenReturn(mockDriverSpec);
    when(mockSparkSpec.getExecutor()).thenReturn(mockExecutorSpec);
    when(mockDriverSpec.getVolumeMounts()).thenReturn(driverVolumeMounts);
    when(mockExecutorSpec.getVolumeMounts()).thenReturn(executorVolumeMounts);

    // Mock AppConfig and NotaryAppConfig
    mockAppConfig = mock(AppConfig.class);
    mockNotaryAppConfig = mock(AppConfig.NotaryAppConfig.class);

    when(mockAppConfig.getNotary()).thenReturn(mockNotaryAppConfig);
    when(mockNotaryAppConfig.getAppNameSpace()).thenReturn("namespace_value");
    when(mockNotaryAppConfig.getNarrativeTuriPodDomain()).thenReturn("pod_domain_value");

    // Mock AppConfig.QueueConfig and set up a list for it
    mockQueueConfig = mock(AppConfig.QueueConfig.class);
    when(mockQueueConfig.getName()).thenReturn("test");
    when(mockQueueConfig.getNotaryEnabled()).thenReturn(true);

    List<AppConfig.QueueConfig> queueConfigs = new ArrayList<>();
    queueConfigs.add(mockQueueConfig);
    when(mockAppConfig.getQueues()).thenReturn(queueConfigs);

    // Other setups
    mockRequestContext = mock(ContainerRequestContext.class);
    when(mockRequestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn("identity_type_value");
    if (SharedMetricRegistries.tryGetDefault() == null) {
      SharedMetricRegistries.setDefault("default", new MetricRegistry());
    }

    applicationSubmissionRest =
        new ApplicationSubmissionRest(mockAppConfig, new SimpleMeterRegistry());
  }

  @Test
  public void testEnableNotaryFeaturesWithPersonId() {

    try (MockedStatic<NotaryPersonIdUtil> mockedStatic = mockStatic(NotaryPersonIdUtil.class)) {
      mockedStatic
          .when(() -> NotaryPersonIdUtil.extractPersonId(mockRequestContext))
          .thenReturn(Optional.of("12345"));

      applicationSubmissionRest.enableNotaryFeatures(
          true, mockSparkSpec, mockRequestContext, mockAppConfig, mockQueueConfig);

      // Verify that the static method was called
      mockedStatic.verify(() -> NotaryPersonIdUtil.extractPersonId(mockRequestContext));

      // Verify the other interactions
      verify(mockQueueConfig).getNotaryEnabled();
      // Add more verifications for the methods called inside the if block
    }
  }

  @Test
  public void testEnableNotaryFeaturesWithEmptyPersonId() {

    try (MockedStatic<NotaryPersonIdUtil> mockedStatic = mockStatic(NotaryPersonIdUtil.class)) {
      mockedStatic
          .when(() -> NotaryPersonIdUtil.extractPersonId(mockRequestContext))
          .thenReturn(Optional.empty());

      applicationSubmissionRest.enableNotaryFeatures(
          true, mockSparkSpec, mockRequestContext, mockAppConfig, mockQueueConfig);

      // Verify that the static method was called
      mockedStatic.verify(() -> NotaryPersonIdUtil.extractPersonId(mockRequestContext));

      // Verify the other interactions
      verify(mockQueueConfig).getNotaryEnabled();
      // Add more verifications for the methods called inside the if block
    }
  }

  @Test
  public void testProcessNotaryApplicationWithFlagTrue() {
    // Set the notary enabled flag to true in the mockQueueConfig
    when(mockQueueConfig.getNotaryEnabled()).thenReturn(true);

    try (MockedStatic<NotaryPersonIdUtil> mockedStatic = mockStatic(NotaryPersonIdUtil.class)) {
      mockedStatic
          .when(() -> NotaryPersonIdUtil.extractPersonId(mockRequestContext))
          .thenReturn(Optional.of("12345"));

      applicationSubmissionRest.enableNotaryFeatures(
          true, mockSparkSpec, mockRequestContext, mockAppConfig, mockQueueConfig);

      // Verify that the static method was called
      mockedStatic.verify(() -> NotaryPersonIdUtil.extractPersonId(mockRequestContext));

      // Verify the other interactions
      verify(mockQueueConfig).getNotaryEnabled();
      // Add more verifications for the methods called inside the if block
    }
  }

  @Test
  public void testEnableNotaryFeaturesWithFlagFalse() {
    // Set the notary enabled flag to true in the mockQueueConfig
    when(mockQueueConfig.getNotaryEnabled()).thenReturn(false);

    applicationSubmissionRest.enableNotaryFeatures(
        false, mockSparkSpec, mockRequestContext, mockAppConfig, mockQueueConfig);

    // Create a mocked static context for NotaryPersonIdUtil
    try (MockedStatic<NotaryPersonIdUtil> mockedStatic = mockStatic(NotaryPersonIdUtil.class)) {
      // Perform the operation that would trigger the method call
      applicationSubmissionRest.enableNotaryFeatures(
          false, mockSparkSpec, mockRequestContext, mockAppConfig, mockQueueConfig);

      // Verify that the extractPersonId method was never called
      mockedStatic.verify(() -> NotaryPersonIdUtil.extractPersonId(mockRequestContext), never());
    }
  }

  @After
  public void tearDown() {
    SharedMetricRegistries.remove("default");
  }
}
