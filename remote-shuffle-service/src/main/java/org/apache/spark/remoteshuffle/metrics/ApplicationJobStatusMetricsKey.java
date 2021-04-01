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

public class ApplicationJobStatusMetricsKey {
  private String user;
  private String jobStatus;

  public ApplicationJobStatusMetricsKey(String user, String jobStatus) {
    this.user = user;
    this.jobStatus = jobStatus;
  }

  public String getUser() {
    return user;
  }

  public String getJobStatus() {
    return jobStatus;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ApplicationJobStatusMetricsKey that = (ApplicationJobStatusMetricsKey) o;
    return Objects.equals(user, that.user) &&
        Objects.equals(jobStatus, that.jobStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hash(user, jobStatus);
  }

  @Override
  public String toString() {
    return "ApplicationJobStatusMetricsKey{" +
        "user='" + user + '\'' +
        ", jobStatus='" + jobStatus + '\'' +
        '}';
  }
}
