/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.util;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests for {@link TimerWithCustomTimeUnit} */
public class TimerWithCustomUnitSuite {

  private static final double EPSILON = 1.0 / 1_000_000_000;

  @Test
  public void testTimerWithMillisecondTimeUnit() {
    testTimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);
  }

  @Test
  public void testTimerWithNanosecondTimeUnit() {
    testTimerWithCustomTimeUnit(TimeUnit.NANOSECONDS);
  }

  private void testTimerWithCustomTimeUnit(TimeUnit timeUnit) {
    Timer timer = new TimerWithCustomTimeUnit(timeUnit);
    Duration[] durations = {
        Duration.ofNanos(1),
        Duration.ofMillis(1),
        Duration.ofMillis(5),
        Duration.ofMillis(100),
        Duration.ofSeconds(10)
    };
    Arrays.stream(durations).forEach(timer::update);

    Snapshot snapshot = timer.getSnapshot();
    assertEquals(toTimeUnit(durations[0], timeUnit), snapshot.getMin());
    assertEquals(toTimeUnitFloating(durations[0], timeUnit), snapshot.getValue(0), EPSILON);
    assertEquals(toTimeUnitFloating(durations[2], timeUnit), snapshot.getMedian(), EPSILON);
    assertEquals(toTimeUnitFloating(durations[3], timeUnit), snapshot.get75thPercentile(), EPSILON);
    assertEquals(toTimeUnit(durations[4], timeUnit), snapshot.getMax());

    assertArrayEquals(Arrays.stream(durations).mapToLong(d -> toTimeUnit(d, timeUnit)).toArray(),
        snapshot.getValues());
    double total = Arrays.stream(durations).mapToDouble(d -> toTimeUnitFloating(d, timeUnit)).sum();
    assertEquals(total / durations.length, snapshot.getMean(), EPSILON);
  }

  @Test
  public void testTimingViaContext() {
    ManualClock clock = new ManualClock();
    Timer timer = new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS, clock);
    Duration[] durations = { Duration.ofNanos(1), Duration.ofMillis(100), Duration.ofMillis(1000) };
    for (Duration d : durations) {
      Timer.Context context = timer.time();
      clock.advance(toTimeUnit(d, TimeUnit.NANOSECONDS));
      context.stop();
    }

    Snapshot snapshot = timer.getSnapshot();
    assertEquals(0, snapshot.getMin());
    assertEquals(100, snapshot.getMedian(), EPSILON);
    assertEquals(1000, snapshot.getMax(), EPSILON);
  }

  private static long toTimeUnit(Duration duration, TimeUnit timeUnit) {
    return timeUnit.convert(duration.toNanos(), TimeUnit.NANOSECONDS);
  }

  private static double toTimeUnitFloating(Duration duration, TimeUnit timeUnit) {
    return ((double) duration.toNanos()) / timeUnit.toNanos(1);
  }

  private static class ManualClock extends Clock {

    private long currTick = 1;

    void advance(long nanos) {
      currTick += nanos;
    }

    @Override
    public long getTick() {
      return currTick;
    }
  }
}
