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
import com.uber.m3.tally.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ExceptionMetrics extends MetricGroup<ExceptionMetricsKey> {
  private static final Logger logger = LoggerFactory.getLogger(ExceptionMetrics.class);

  private final Counter numExceptions;

  public ExceptionMetrics(String exceptionName, String exceptionSource) {
    super(new ExceptionMetricsKey(exceptionName, exceptionSource));
    this.numExceptions = scope.counter("numExceptions");
  }

  public Counter getNumExceptions() {
    return numExceptions;
  }

  @Override
  public void close() {
    M3Stats.decreaseNumM3Scopes();
  }

  @Override
  protected Scope createScope(ExceptionMetricsKey key) {
    Map<String, String> tags = new HashMap<>();
    tags.put("exception", key.getExceptionName());
    tags.put("source", key.getExceptionSource());
    return M3Stats.createSubScope(tags);
  }
}
