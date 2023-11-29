package com.apple.spark.initContainer;

public abstract class BaseException extends Exception {

  private final int exceptionCode;

  public BaseException(int exceptionCode, String messsage, Throwable t) {
    super(messsage, t);
    this.exceptionCode = exceptionCode;
  }

  public BaseException(int exceptionCode, String messsage) {
    super(messsage);
    this.exceptionCode = exceptionCode;
  }

  public int getExceptionCode() {
    return exceptionCode;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(" : ");
    sb.append(getExceptionCode());
    return sb.toString();
  }
}
