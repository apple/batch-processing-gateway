package com.apple.spark.crd.costattrib;

import com.apple.spark.core.KubernetesHelper;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class is to help retrieve cost attribution resource from CRD */
public class CostAttributionHelper {
  private static final Logger logger = LoggerFactory.getLogger(CostAttributionHelper.class);

  private static CostAttributionResourceList tryGetCostAttributionCRDFromK8s(
      String gatewayNamespace) {
    if (gatewayNamespace == null || gatewayNamespace.isEmpty()) {
      gatewayNamespace = KubernetesHelper.tryGetServiceAccountNamespace();
    }

    if (gatewayNamespace == null || gatewayNamespace.isEmpty()) {
      gatewayNamespace = CostAttributionConstants.DEFAULT_GATEWAY_NAMESPACE;
    }

    logger.info(String.format("tryGetCostAttributionCRDFromK8s: %s", gatewayNamespace));
    int retryTimes = 2;
    int retryIntervalMillis = 1000;
    for (int i = 0; i <= retryTimes; i++) {
      try (KubernetesClient client = KubernetesHelper.getLocalK8sClient()) {
        MixedOperation<CostAttribution, CostAttributionResourceList, Resource<CostAttribution>>
            costAttributionClient =
                client.resources(CostAttribution.class, CostAttributionResourceList.class);

        CostAttributionResourceList carList =
            costAttributionClient.inNamespace(gatewayNamespace).list();

        if (carList == null) {
          logger.warn(
              String.format("Cost attribution CRD not exists in namespace %s ", gatewayNamespace));
          return null;
        }
        logger.info(String.format("Found cost attribution CRD in namespace %s ", gatewayNamespace));
        return carList;
      } catch (Throwable ex) {
        logger.warn(
            String.format("Failed to get cost attribution CRD in namespace %s", gatewayNamespace),
            ex);
        try {
          Thread.sleep(retryIntervalMillis);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return null;
  }

  public static List<CostAttributionSpec> getCostAttributionConfigSpecList(
      String gatewayNamespace) {
    CostAttributionResourceList list = tryGetCostAttributionCRDFromK8s(gatewayNamespace);
    List<CostAttributionSpec> specList = new ArrayList<>();
    if (list != null) {
      for (CostAttribution resource : list.getItems()) {
        specList.add(resource.getSpec());
      }
    }
    return specList;
  }
}
