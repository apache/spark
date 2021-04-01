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

import com.uber.m3.tally.*;
import com.uber.m3.util.Duration;

import java.util.Map;

public class M3DummyScope implements Scope {
  @Override
  public Counter counter(String s) {
    return new Counter() {
      @Override
      public void inc(long l) {
      }
    };
  }

  @Override
  public Gauge gauge(String s) {
    return new Gauge() {
      @Override
      public void update(double v) {
      }
    };
  }

  @Override
  public Timer timer(String s) {
    return new Timer() {
      @Override
      public void record(Duration duration) {
      }

      @Override
      public Stopwatch start() {
        StopwatchRecorder stopwatchRecorder = new StopwatchRecorder() {
          @Override
          public void recordStopwatch(long l) {
          }
        };
        return new Stopwatch(System.nanoTime(), stopwatchRecorder);
      }
    };
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Histogram histogram(String s, Buckets buckets) {
    return new Histogram() {
      @Override
      public void recordValue(double v) {
      }

      @Override
      public void recordDuration(Duration duration) {
      }

      @Override
      public Stopwatch start() {
        StopwatchRecorder stopwatchRecorder = new StopwatchRecorder() {
          @Override
          public void recordStopwatch(long l) {
          }
        };
        return new Stopwatch(System.nanoTime(), stopwatchRecorder);
      }
    };
  }

  @Override
  public Scope tagged(Map<String, String> map) {
    return this;
  }

  @Override
  public Scope subScope(String s) {
    return this;
  }

  @Override
  public Capabilities capabilities() {
    return new Capabilities() {
      @Override
      public boolean reporting() {
        return false;
      }

      @Override
      public boolean tagging() {
        return false;
      }
    };
  }

  @Override
  public void close() {
  }
}
