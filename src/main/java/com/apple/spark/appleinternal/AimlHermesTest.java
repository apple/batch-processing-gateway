package com.apple.spark.appleinternal;

import com.apple.spark.util.HttpUtils;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

// This tool gets a GCP web identity token, then use it to get AWS credential.
// See Hermes python code example:
// https://github.pie.apple.com/CloudTech/platform-gcp-hermes/blob/main/examples/gcp_to_aws/python/generate_creds.py
public class AimlHermesTest {
  private static String roleSessionName = "aiml_hermes_test";
  private static String assumeRoleArn = "";
  private static int durationSeconds = 3600;

  public static void main(String[] args) {
    for (int i = 0; i < args.length; ) {
      String argName = args[i++];
      if (argName.equalsIgnoreCase("-role-session-name")) {
        roleSessionName = args[i++];
      } else if (argName.equalsIgnoreCase("-assume-role-arn")) {
        assumeRoleArn = args[i++];
      } else if (argName.equalsIgnoreCase("-token-duration-seconds")) {
        durationSeconds = Integer.parseInt(args[i++]);
      } else {
        throw new RuntimeException(String.format("Unsupported argument: %s", argName));
      }
    }

    if (assumeRoleArn.isEmpty()) {
      throw new RuntimeException("Empty assumeRoleArn");
    }

    String gcpWebIdentityToken = getGcpWebIdentityToken();
    String awsCredential = getAwsCredential(gcpWebIdentityToken);
    System.out.println(awsCredential);
  }

  private static String getGcpWebIdentityToken() {
    String url =
        "http://metadata/computeMetadata/v1/instance/service-accounts/default/identity?audience=''";
    String responseStr = HttpUtils.get(url, "Metadata-Flavor", "Google");
    System.out.println(String.format("Got response from url %s: %s", url, responseStr));
    return responseStr;
  }

  private static String getAwsCredential(String gcpWebIdentityToken) {
    String url =
        String.format(
            "https://sts.amazonaws.com/?Action=AssumeRoleWithWebIdentity"
                + "&RoleSessionName=%s"
                + "&RoleArn=%s"
                + "&WebIdentityToken=%s"
                + "&Version=2011-06-15"
                + "&DurationSeconds=%s",
            URLEncoder.encode(roleSessionName, StandardCharsets.UTF_8),
            URLEncoder.encode(assumeRoleArn, StandardCharsets.UTF_8),
            URLEncoder.encode(gcpWebIdentityToken, StandardCharsets.UTF_8),
            durationSeconds);
    String responseStr = HttpUtils.get(url, null, null);
    System.out.println(String.format("Got response from url %s: %s", url, responseStr));
    return responseStr;
  }
}
