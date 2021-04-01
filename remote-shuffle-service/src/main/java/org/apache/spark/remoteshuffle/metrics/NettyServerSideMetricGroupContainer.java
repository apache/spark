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

import java.util.function.Function;

public class NettyServerSideMetricGroupContainer<M extends MetricGroup<NettyServerSideMetricsKey>> {
  private MetricGroupContainer<NettyServerSideMetricsKey, M> metricGroupContainer;

  public NettyServerSideMetricGroupContainer(
      Function<NettyServerSideMetricsKey, ? extends M> createFunction) {
    this.metricGroupContainer =
        new MetricGroupContainer<NettyServerSideMetricsKey, M>(createFunction);
  }

  public M getMetricGroup(String user) {
    return metricGroupContainer.getMetricGroup(new NettyServerSideMetricsKey(user));
  }

  public void removeMetricGroup(String user) {
    metricGroupContainer.removeMetricGroup(new NettyServerSideMetricsKey(user));
  }
}
