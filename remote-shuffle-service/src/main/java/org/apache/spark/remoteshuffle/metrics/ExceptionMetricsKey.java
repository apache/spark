/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.metrics;

import java.util.Objects;

public class ExceptionMetricsKey {
  private final String exceptionName;
  private final String exceptionSource;

  public ExceptionMetricsKey(String exceptionName, String exceptionSource) {
    this.exceptionName = exceptionName;
    this.exceptionSource = exceptionSource;
  }

  public String getExceptionName() {
    return exceptionName;
  }

  public String getExceptionSource() {
    return exceptionSource;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExceptionMetricsKey that = (ExceptionMetricsKey) o;
    return Objects.equals(exceptionName, that.exceptionName) &&
        Objects.equals(exceptionSource, that.exceptionSource);
  }

  @Override
  public int hashCode() {
    return Objects.hash(exceptionName, exceptionSource);
  }

  @Override
  public String toString() {
    return "ExceptionMetricsKey{" +
        "exceptionName='" + exceptionName + '\'' +
        ", exceptionSource='" + exceptionSource + '\'' +
        '}';
  }
}
