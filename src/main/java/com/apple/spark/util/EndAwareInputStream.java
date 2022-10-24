/*
 *
 * This source file is part of the Batch Processing Gateway open source project
 *
 * Copyright 2022 Apple Inc. and the Batch Processing Gateway project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.spark.util;

import java.io.IOException;
import java.io.InputStream;

/** This class wraps an InputStream and executes extra action when InputStream reads to the end. */
public class EndAwareInputStream extends InputStream {

  private final InputStream inputStream;
  private final Runnable endingAction;

  public EndAwareInputStream(InputStream inputStream, Runnable endingAction) {
    this.inputStream = inputStream;
    this.endingAction = endingAction;
  }

  @Override
  public int read() throws IOException {
    int result = executeWithResultValue(inputStream::read);
    if (result == -1) {
      executeEndingAction();
    }
    return result;
  }

  @Override
  public int read(byte[] b) throws IOException {
    int result = executeWithResultValue(() -> inputStream.read(b));
    if (result == -1) {
      executeEndingAction();
    }
    return result;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int result = executeWithResultValue(() -> inputStream.read(b, off, len));
    if (result == -1) {
      executeEndingAction();
    }
    return result;
  }

  @Override
  public long skip(long n) throws IOException {
    return executeWithResultValue(() -> inputStream.skip(n));
  }

  @Override
  public int available() throws IOException {
    return executeWithResultValue(() -> inputStream.available());
  }

  @Override
  public void close() throws IOException {
    executeWithoutResultValue(() -> inputStream.close());
  }

  @Override
  public void mark(int readlimit) {
    inputStream.mark(readlimit);
  }

  @Override
  public void reset() throws IOException {
    executeWithoutResultValue(() -> inputStream.reset());
  }

  @Override
  public boolean markSupported() {
    return inputStream.markSupported();
  }

  private <T> T executeWithResultValue(CallableWithIOException<T> callable) throws IOException {
    try {
      return callable.call();
    } catch (IOException ex) {
      executeEndingAction();
      throw ex;
    }
  }

  private void executeWithoutResultValue(RunnableWithIOException callable) throws IOException {
    try {
      callable.run();
    } catch (IOException ex) {
      executeEndingAction();
      throw ex;
    }
  }

  private void executeEndingAction() {
    if (endingAction != null) {
      endingAction.run();
    }
  }

  private interface CallableWithIOException<T> {

    T call() throws IOException;
  }

  private interface RunnableWithIOException {

    void run() throws IOException;
  }
}
