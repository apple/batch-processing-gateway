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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class HttpUtils {

  public static <T> T post(
          String url, String requestJson, String headerName, String headerValue, Class<T> clazz) {
    return post(url, requestJson, "application/json", headerName, headerValue, clazz);
  }

  public static <T> T post(
          String url,
          String requestText,
          String contentType,
          String headerName,
          String headerValue,
          Class<T> clazz) {
    String str = post(url, requestText, contentType, headerName, headerValue);
    return parseJson(str, clazz);
  }

  public static String post(String url, String requestJson, String headerName, String headerValue) {
    return post(url, requestJson, "application/json", headerName, headerValue);
  }

  public static String post(
          String url, String requestText, String contentType, String headerName, String headerValue) {
    HttpRequest request = null;
    try {
      var builder = HttpRequest.newBuilder().uri(new URI(url)).header("Content-Type", contentType);
      if (headerName != null && !headerName.isEmpty()) {
        builder = builder.header(headerName, headerValue);
      }
      request = builder.POST(HttpRequest.BodyPublishers.ofString(requestText)).build();
      return executeHttpRequest(url, request, HttpResponse.BodyHandlers.ofString());
    } catch (Throwable ex) {
      throw new RuntimeException(
              String.format("Failed to execute %s on %s", request.method(), url), ex);
    }
  }

  public static <T> T post(
          String url, InputStream stream, String headerName, String headerValue, Class<T> clazz) {
    String str = post(url, stream, headerName, headerValue);
    return parseJson(str, clazz);
  }

  public static <T> T post(
          String url,
          InputStream stream,
          String headerName,
          String headerValue,
          long contentLength,
          Class<T> clazz) {
    String str = post(url, stream, headerName, headerValue, contentLength);
    return parseJson(str, clazz);
  }

  public static String post(String url, InputStream stream, String headerName, String headerValue) {
    return post(url, stream, headerName, headerValue, -1);
  }

  public static String post(
          String url, InputStream stream, String headerName, String headerValue, long contentLength) {
    HttpRequest request = null;
    try {
      var builder = HttpRequest.newBuilder().uri(new URI(url));
      if (headerName != null && !headerName.isEmpty()) {
        builder = builder.header(headerName, headerValue);
      }
      request = builder.POST(HttpRequest.BodyPublishers.ofInputStream(() -> stream)).build();
      return executeHttpRequest(url, request, HttpResponse.BodyHandlers.ofString());
    } catch (Throwable ex) {
      throw new RuntimeException(
              String.format("Failed to execute %s on %s", request.method(), url), ex);
    }
  }

  public static <T> T get(String url, String headerName, String headerValue, Class<T> clazz) {
    String str = get(url, headerName, headerValue);
    return parseJson(str, clazz);
  }

  public static String get(String url, String headerName, String headerValue) {
    try {
      var builder = HttpRequest.newBuilder().uri(new URI(url));
      if (headerName != null && !headerName.isEmpty()) {
        builder = builder.header(headerName, headerValue);
      }
      HttpRequest request = builder.GET().build();
      HttpResponse<String> response =
              HttpClient.newBuilder().build().send(request, HttpResponse.BodyHandlers.ofString());
      return response.body();
    } catch (Throwable ex) {
      throw new RuntimeException(String.format("Failed to get from %s", url), ex);
    }
  }

  public static String get(String url) {
    return get(url, null, null);
  }

  public static <T> T delete(String url, String headerName, String headerValue, Class<T> clazz) {
    String str = delete(url, headerName, headerValue);
    return parseJson(str, clazz);
  }

  public static String delete(String url, String headerName, String headerValue) {
    try {
      var builder = HttpRequest.newBuilder().uri(new URI(url));
      if (headerName != null && !headerName.isEmpty()) {
        builder = builder.header(headerName, headerValue);
      }
      HttpRequest request = builder.DELETE().build();
      HttpResponse<String> response =
              HttpClient.newBuilder().build().send(request, HttpResponse.BodyHandlers.ofString());
      return response.body();
    } catch (Throwable ex) {
      throw new RuntimeException(String.format("Failed to delete %s", url), ex);
    }
  }

  private static void checkResponseOK(String url, HttpResponse<?> response) {
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      String responseBodyStr;
      try {
        responseBodyStr = response.body().toString(); // TODO modify this
      } catch (Throwable e) {
        responseBodyStr =
                String.format(
                        "(Failed to get response body, exception: %s)",
                        ExceptionUtils.getExceptionNameAndMessage(e));
      }
      throw new RuntimeException(
              String.format(
                      "Response for %s is not OK: %s. Response body: %s",
                      url, response.statusCode(), responseBodyStr));
    }
  }

  private static <T> T parseJson(String str, Class<T> clazz) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.readValue(str, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(String.format("Failed to parse json: %s", str), e);
    }
  }

  public static HttpResponse getHttpResponse(String url, String headerName, String headerValue) {
    HttpResponse<String> response;
    try {
      var builder = HttpRequest.newBuilder().uri(new URI(url));
      if (headerName != null && !headerName.isEmpty()) {
        builder = builder.header(headerName, headerValue);
      }
      HttpRequest request = builder.GET().build();
      response =
              HttpClient.newBuilder().build().send(request, HttpResponse.BodyHandlers.ofString());
    } catch (Throwable ex) {
      throw new RuntimeException(String.format("Failed to get request from %s", url), ex);
    }

    try {
      checkResponseOK(url, response);
    } catch (RuntimeException ex) {
      throw ex;
    }
    return response;
  }

  private static String executeHttpRequest(
          String url, HttpRequest request, HttpResponse.BodyHandler<String> bodyHandler) {
    try {
      HttpResponse<String> response = HttpClient.newBuilder().build().send(request, bodyHandler);
      checkResponseOK(url, response);
      String responseStr = response.body();
      return responseStr;
    } catch (Throwable ex) {
      throw new RuntimeException(
              String.format("Failed to execute %s on %s", request.method(), url), ex);
    }
  }
}
