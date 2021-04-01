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

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Timer;

import java.util.HashMap;
import java.util.Map;

public class WriteClientMetrics extends MetricGroup<WriteClientMetricsKey> {

  private final Counter numClients;
  private final Counter numWriteBytes;
  private final Counter numRetries;
  private final Timer writeConnectLatency;
  private final Timer finishUploadLatency;

  private final Gauge bufferSize;

  public WriteClientMetrics(WriteClientMetricsKey key) {
    super(key);

    // The name like "numClients" was used when there were a lot of metric series which caused M3 issue, e.g.
    // not able to load the values in dashboard. Use new names ending with 3 here.
    this.numClients = scope.counter("numClients4");
    this.numWriteBytes = scope.counter("numWriteBytes4");
    this.numRetries = scope.counter("numRetries4");
    this.writeConnectLatency = scope.timer("writeConnectLatency4");
    this.finishUploadLatency = scope.timer("finishUploadLatency4");
    this.bufferSize = scope.gauge("bufferSize4");
  }

  public Counter getNumClients() {
    return numClients;
  }

  public Counter getNumWriteBytes() {
    return numWriteBytes;
  }

  public Counter getNumRetries() {
    return numRetries;
  }

  public Timer getWriteConnectLatency() {
    return writeConnectLatency;
  }

  public Timer getFinishUploadLatency() {
    return finishUploadLatency;
  }

  public Gauge getBufferSize() {
    return bufferSize;
  }

  @Override
  protected Scope createScope(WriteClientMetricsKey key) {
    Map<String, String> tags = new HashMap<>();
    tags.put(M3Stats.TAG_NAME_SOURCE, key.getSource());
    tags.put(M3Stats.TAG_NAME_USER, key.getUser());
    return M3Stats.createSubScope(tags);
  }
}
