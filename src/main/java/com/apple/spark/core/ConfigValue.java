package com.apple.spark.core;

public class ConfigValue {

  private static final String PLAINTEXT_PREFIX = "plaintext:";

  private static final String LOCALHOST_ENV_PREFIX = "localhost:env:";

  private ConfigValue() {}

  /***
   * This method tries to get secret value from an encoded string.
   * Following are examples for encoded string:
   * plaintext:value123 - get plaintext value like value123
   * localhost:env:my_env_variable_name - get value from environment variable my_env_variable_name
   *
   * If the value is not in recognized encoding, this method will return the value directly.
   * @param value
   * @return
   */
  public static String tryGetEncodedSecretValue(String value) {
    if (value == null || value.isEmpty()) {
      return value;
    }
    if (value.startsWith(PLAINTEXT_PREFIX)) {
      return value.substring(PLAINTEXT_PREFIX.length());
    } else if (value.startsWith(LOCALHOST_ENV_PREFIX)) {
      String envVarName = value.substring(LOCALHOST_ENV_PREFIX.length());
      value = System.getenv(envVarName);
      if (value == null) {
        return "";
      } else {
        return value;
      }
    }
    return value;
  }
}
