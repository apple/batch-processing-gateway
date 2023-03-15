package com.apple.spark.crd;

import com.apple.spark.core.KubernetesHelper;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.util.ArrayList;
import java.util.List;

/** This class is to help retrieve spark cluster resource from CRD */
public class VirtualSparkClusterHelper {
  private static VirtualSparkClusterResourceList getVirtualSparkClusterConfigResources(
      String gatewayNamespace) {
    try (KubernetesClient client = KubernetesHelper.getLocalK8sClient()) {
      MixedOperation<
              VirtualSparkCluster, VirtualSparkClusterResourceList, Resource<VirtualSparkCluster>>
          virtualSparkClusterClient =
              client.resources(VirtualSparkCluster.class, VirtualSparkClusterResourceList.class);

      if (gatewayNamespace == null || gatewayNamespace.isEmpty()) {
        gatewayNamespace = KubernetesHelper.tryGetServiceAccountNamespace();
      }

      if (gatewayNamespace == null || gatewayNamespace.isEmpty()) {
        gatewayNamespace = VirtualSparkClusterConstants.DEFAULT_GATEWAY_NAMESPACE;
      }

      VirtualSparkClusterResourceList list =
          virtualSparkClusterClient.inNamespace(gatewayNamespace).list();

      return list != null ? list : new VirtualSparkClusterResourceList();
    }
  }

  public static List<VirtualSparkClusterSpec> getVirtualSparkClusterConfigSpecList(
      String gatewayNamespace) {
    VirtualSparkClusterResourceList list = getVirtualSparkClusterConfigResources(gatewayNamespace);

    List<VirtualSparkClusterSpec> specList = new ArrayList<>();

    for (VirtualSparkCluster resource : list.getItems()) {
      specList.add(resource.getSpec());
    }
    return specList;
  }
}
