package com.apple.spark.tools;

import com.apple.spark.AppConfig;
import com.apple.spark.core.SparkClusterHelper;
import com.apple.spark.crd.SparkClusterCrdDiscovery;
import com.apple.spark.crd.VirtualSparkClusterSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import java.io.IOException;
import java.util.List;

/***
 * This tool is to check VirtualSparkCluster CRD in the given namespace.
 */
public class VirtualSparkClusterCrdTest {
  public static void main(String[] args) throws IOException {
    String namespace = "";

    for (int i = 0; i < args.length; ) {
      String argName = args[i++];
      if (argName.equalsIgnoreCase("-namespace")) {
        namespace = args[i++];
      } else {
        throw new RuntimeException(String.format("Unsupported argument: %s", argName));
      }
    }

    List<VirtualSparkClusterSpec> list =
        SparkClusterCrdDiscovery.getInstance().getClusters(namespace);
    printClusters(list);

    AppConfig appConfig = new AppConfig();
    appConfig.setGatewayNamespace(namespace);
    list = SparkClusterHelper.concatenateSparkClusters(appConfig);
    printClusters(list);
  }

  private static void printClusters(List<VirtualSparkClusterSpec> list) throws IOException {
    for (VirtualSparkClusterSpec item : list) {
      ObjectMapper objectMapper =
          new ObjectMapper(
              new YAMLFactory()
                  .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                  .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
      String yamlStr = objectMapper.writeValueAsString(item);
      System.out.println("--- VirtualSparkCluster ---\n" + yamlStr);
    }
  }
}
