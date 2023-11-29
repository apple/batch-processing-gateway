package com.apple.spark.initContainer;

public class ConfigException extends BaseException {

  public ConfigException(int exceptionCode, String messsage) {
    this(exceptionCode, messsage, null);
  }

  public ConfigException(int exceptionCode, String messsage, Throwable t) {
    super(exceptionCode, messsage, t);
  }
}
