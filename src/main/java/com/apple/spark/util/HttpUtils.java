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
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;

public class HttpUtils {

  public static <T> T post(
      String url, String requestJson, String headerName, String headerValue, Class<T> clazz) {
    MediaType mediaType = MediaType.parse("application/json");
    return post(url, requestJson, mediaType, headerName, headerValue, clazz);
  }

  public static <T> T post(
      String url,
      String requestText,
      MediaType mediaType,
      String headerName,
      String headerValue,
      Class<T> clazz) {
    String str = post(url, requestText, mediaType, headerName, headerValue);
    return parseJson(str, clazz);
  }

  public static String post(String url, String requestJson, String headerName, String headerValue) {
    MediaType mediaType = MediaType.parse("application/json");
    return post(url, requestJson, mediaType, headerName, headerValue);
  }

  public static String post(
      String url, String requestText, MediaType mediaType, String headerName, String headerValue) {
    OkHttpClient client = getOkHttpClient();
    RequestBody body = RequestBody.create(mediaType, requestText);
    Request request =
        new Request.Builder().url(url).header(headerName, headerValue).post(body).build();
    return executeHttpRequest(url, client, request);
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
    RequestBody requestBody =
        new RequestBody() {
          @Override
          public MediaType contentType() {
            return MediaType.parse("application/octet-stream");
          }

          @Override
          public void writeTo(BufferedSink sink) throws IOException {
            byte[] buffer = new byte[31];
            int size = stream.read(buffer);
            while (size != -1) {
              sink.write(buffer, 0, size);
              size = stream.read(buffer);
            }
          }

          @Override
          public long contentLength() throws IOException {
            return contentLength;
          }
        };

    OkHttpClient client = getOkHttpClient();
    Request request =
        new Request.Builder().url(url).header(headerName, headerValue).post(requestBody).build();
    return executeHttpRequest(url, client, request);
  }

  public static <T> T get(String url, String headerName, String headerValue, Class<T> clazz) {
    String str = get(url, headerName, headerValue);
    return parseJson(str, clazz);
  }

  public static String get(String url, String headerName, String headerValue) {
    OkHttpClient client = getOkHttpClient();
    Request request = new Request.Builder().url(url).header(headerName, headerValue).get().build();
    return executeHttpRequest(url, client, request);
  }

  public static String get(String url) {
    OkHttpClient client = getOkHttpClient();
    Request request = new Request.Builder().url(url).get().build();
    return executeHttpRequest(url, client, request);
  }

  public static <T> T delete(String url, String headerName, String headerValue, Class<T> clazz) {
    String str = delete(url, headerName, headerValue);
    return parseJson(str, clazz);
  }

  public static String delete(String url, String headerName, String headerValue) {
    OkHttpClient client = getOkHttpClient();
    Request request =
        new Request.Builder().url(url).header(headerName, headerValue).delete().build();
    return executeHttpRequest(url, client, request);
  }

  public static Response getHttpResponse(String url, String headerName, String headerValue) {
    OkHttpClient client = getOkHttpClient();
    Request request = new Request.Builder().url(url).header(headerName, headerValue).get().build();
    Response response = null;
    try {
      response = client.newCall(request).execute();
    } catch (IOException ex) {
      throw new RuntimeException(String.format("Failed to get request to %s", url), ex);
    }
    try {
      checkResponseOK(url, response);
    } catch (RuntimeException ex) {
      response.close();
      throw ex;
    }
    return response;
  }

  private static OkHttpClient getOkHttpClient() {
    X509TrustManager trustManager =
        new X509TrustManager() {
          @Override
          public void checkClientTrusted(X509Certificate[] chain, String authType)
              throws CertificateException {}

          @Override
          public void checkServerTrusted(X509Certificate[] chain, String authType)
              throws CertificateException {}

          @Override
          public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
          }
        };

    try {
      SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(null, new TrustManager[] {trustManager}, new java.security.SecureRandom());
      SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

      OkHttpClient.Builder builder = new OkHttpClient.Builder();
      builder.sslSocketFactory(sslSocketFactory, trustManager);
      return builder
          .connectTimeout(90, TimeUnit.SECONDS)
          .writeTimeout(90, TimeUnit.SECONDS)
          .readTimeout(90, TimeUnit.SECONDS)
          .build();
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Failed to create OkHttp client", e);
    }
  }

  private static String executeHttpRequest(String url, OkHttpClient client, Request request) {
    try (Response response = client.newCall(request).execute()) {
      checkResponseOK(url, response);
      String responseStr = response.body().string();
      return responseStr;
    } catch (Throwable ex) {
      throw new RuntimeException(
          String.format("Failed to execute %s on %s", request.method(), url), ex);
    }
  }

  private static void checkResponseOK(String url, Response response) {
    if (response.code() < 200 || response.code() >= 300) {
      String responseBodyStr;
      try {
        responseBodyStr = response.body().string();
      } catch (IOException e) {
        responseBodyStr =
            String.format(
                "(Failed to get response body, exception: %s)",
                ExceptionUtils.getExceptionNameAndMessage(e));
      }
      throw new RuntimeException(
          String.format(
              "Response for %s is not OK: %s. Response body: %s",
              url, response.code(), responseBodyStr));
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
}
