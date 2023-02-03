package com.apple.spark.crd;

import com.apple.spark.AppConfig;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import java.util.ArrayList;
import java.util.List;

/** This class is to help retrieve spark cluster resource from CRD */
public class VirtualSparkClusterHelper {
  public static VirtualSparkClusterResourceList getVirtualSparkClusterConfigResources() {
    DefaultKubernetesClient client = new DefaultKubernetesClient();

    AppConfig appConfig = new AppConfig();

    CustomResourceDefinitionContext crdContext =
        VirtualSparkClusterHelper.getVirtualSparkClusterConfigContext();

    VirtualSparkClusterResourceList list =
        client
            .customResources(
                crdContext,
                VirtualSparkClusterResource.class,
                VirtualSparkClusterResourceList.class,
                VirtualSparkClusterResourceDoneable.class)
            .inNamespace(appConfig.getGatewayNamespace())
            .list();
    return list != null ? list : new VirtualSparkClusterResourceList();
  }

  public static List<VirtualSparkClusterSpec> getVirtualSparkClusterConfigSpec() {
    VirtualSparkClusterResourceList list = getVirtualSparkClusterConfigResources();

    List<VirtualSparkClusterSpec> specList = new ArrayList<>();

    for (VirtualSparkClusterResource resource : list.getItems()) {
      specList.add(resource.getSpec());
    }
    return specList;
  }

  public static CustomResourceDefinitionContext getVirtualSparkClusterConfigContext() {
    return new CustomResourceDefinitionContext.Builder()
        .withGroup(VirtualSparkClusterConstants.VIRTUAL_SPARK_CLUSTER_CRD_GROUP)
        .withScope(VirtualSparkClusterConstants.CRD_SCOPE)
        .withVersion(VirtualSparkClusterConstants.CRD_VERSION)
        .withPlural(VirtualSparkClusterConstants.VIRTUAL_SPARK_CLUSTER_CRD_PLURAL)
        .build();
  }
}
