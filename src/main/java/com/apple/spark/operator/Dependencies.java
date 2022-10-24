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

package com.apple.spark.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Dependencies {

  private List<String> jars;
  private List<String> files;
  private List<String> pyFiles;
  private List<String> packages;
  private List<String> excludePackages;
  private List<String> repositories;
  private List<String> archives;

  public List<String> getJars() {
    return jars;
  }

  public void setJars(List<String> jars) {
    this.jars = jars;
  }

  public List<String> getFiles() {
    return files;
  }

  public void setFiles(List<String> files) {
    this.files = files;
  }

  public List<String> getPyFiles() {
    return pyFiles;
  }

  public void setPyFiles(List<String> pyFiles) {
    this.pyFiles = pyFiles;
  }

  public List<String> getPackages() {
    return packages;
  }

  public void setPackages(List<String> packages) {
    this.packages = packages;
  }

  public List<String> getExcludePackages() {
    return excludePackages;
  }

  public void setExcludePackages(List<String> excludePackages) {
    this.excludePackages = excludePackages;
  }

  public List<String> getRepositories() {
    return repositories;
  }

  public void setRepositories(List<String> repositories) {
    this.repositories = repositories;
  }

  public List<String> getArchives() {
    return archives;
  }

  public void setArchives(List<String> archives) {
    this.archives = archives;
  }
}
