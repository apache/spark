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

public class MetadataClientMetricsKey {
  private String client;
  private String operation;

  public MetadataClientMetricsKey(String client, String operation) {
    this.client = client;
    this.operation = operation;
  }

  public String getClient() {
    return client;
  }

  public void setClient(String client) {
    this.client = client;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MetadataClientMetricsKey that = (MetadataClientMetricsKey) o;
    return Objects.equals(client, that.client) &&
        Objects.equals(operation, that.operation);
  }

  @Override
  public int hashCode() {

    return Objects.hash(client, operation);
  }

  @Override
  public String toString() {
    return "MetadataClientMetricsKey{" +
        "client='" + client + '\'' +
        ", operation='" + operation + '\'' +
        '}';
  }
}
