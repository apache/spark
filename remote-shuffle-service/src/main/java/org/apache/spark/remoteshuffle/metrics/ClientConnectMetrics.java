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

import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Timer;

import java.util.HashMap;
import java.util.Map;

public class ClientConnectMetrics extends MetricGroup<ClientConnectMetricsKey> {

  private final Timer socketConnectLatency;
  private final Gauge socketConnectRetries;

  public ClientConnectMetrics(ClientConnectMetricsKey key) {
    super(key);

    this.socketConnectLatency = scope.timer("socketConnectLatency");
    this.socketConnectRetries = scope.gauge("socketConnectRetries");
  }

  public Timer getSocketConnectLatency() {
    return socketConnectLatency;
  }

  public Gauge getSocketConnectRetries() {
    return socketConnectRetries;
  }

  @Override
  protected Scope createScope(ClientConnectMetricsKey key) {
    Map<String, String> tags = new HashMap<>();
    tags.put(M3Stats.TAG_NAME_SOURCE, key.getSource());
    tags.put(M3Stats.TAG_NAME_REMOTE, key.getRemote());
    return M3Stats.createSubScope(tags);
  }
}
