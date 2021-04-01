/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
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

public class NotifyServerMetricsContainer {
  private MetricGroupContainer<ApplicationJobStatusMetricsKey, ApplicationJobStatusMetrics>
      applicationJobStatusMetricsContainer;

  private MetricGroupContainer<ApplicationMetricsKey, ApplicationMetrics>
      applicationMetricsContainer;

  public NotifyServerMetricsContainer() {
    this.applicationJobStatusMetricsContainer = new MetricGroupContainer<>(
        t -> new ApplicationJobStatusMetrics(t));

    this.applicationMetricsContainer = new MetricGroupContainer<>(
        t -> new ApplicationMetrics(t));
  }

  public ApplicationJobStatusMetrics getApplicationJobStatusMetrics(String user,
                                                                    String jobStatus) {
    ApplicationJobStatusMetricsKey key = new ApplicationJobStatusMetricsKey(user, jobStatus);
    return applicationJobStatusMetricsContainer.getMetricGroup(key);
  }

  public ApplicationMetrics getApplicationMetrics(String user, String attemptId) {
    ApplicationMetricsKey key = new ApplicationMetricsKey(user, attemptId);
    return applicationMetricsContainer.getMetricGroup(key);
  }
}
