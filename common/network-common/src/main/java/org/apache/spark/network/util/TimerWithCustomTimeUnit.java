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

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * A custom version of a {@link Timer} which allows for specifying a specific {@link TimeUnit} to
 * be used when accessing timing values via {@link #getSnapshot()}. Normally, though the
 * {@link #update(long, TimeUnit)} method requires a unit, the extraction methods on the snapshot
 * do not specify a unit, and always return nanoseconds. It can be useful to specify that a timer
 * should use a different unit for its snapshot. Note that internally, all values are still stored
 * with nanosecond-precision; it is only before being returned to the caller that the nanosecond
 * value is converted to the custom time unit.
 */
public class TimerWithCustomTimeUnit extends Timer {

  private final TimeUnit timeUnit;
  private final double nanosPerUnit;

  public TimerWithCustomTimeUnit(TimeUnit timeUnit) {
    this(timeUnit, Clock.defaultClock());
  }

  TimerWithCustomTimeUnit(TimeUnit timeUnit, Clock clock) {
    super(new ExponentiallyDecayingReservoir(), clock);
    this.timeUnit = timeUnit;
    this.nanosPerUnit = timeUnit.toNanos(1);
  }

  @Override
  public Snapshot getSnapshot() {
    return new SnapshotWithCustomTimeUnit(super.getSnapshot());
  }

  private double toUnit(double nanos) {
    // TimeUnit.convert() truncates (loses precision), so floating-point division is used instead
    return nanos / nanosPerUnit;
  }

  private long toUnit(long nanos) {
    return timeUnit.convert(nanos, TimeUnit.NANOSECONDS);
  }

  private class SnapshotWithCustomTimeUnit extends Snapshot {

    private final Snapshot wrappedSnapshot;

    SnapshotWithCustomTimeUnit(Snapshot wrappedSnapshot) {
      this.wrappedSnapshot = wrappedSnapshot;
    }

    @Override
    public double getValue(double v) {
      return toUnit(wrappedSnapshot.getValue(v));
    }

    @Override
    public long[] getValues() {
      long[] nanoValues = wrappedSnapshot.getValues();
      long[] customUnitValues = new long[nanoValues.length];
      for (int i = 0; i < nanoValues.length; i++) {
        customUnitValues[i] = toUnit(nanoValues[i]);
      }
      return customUnitValues;
    }

    @Override
    public int size() {
      return wrappedSnapshot.size();
    }

    @Override
    public long getMax() {
      return toUnit(wrappedSnapshot.getMax());
    }

    @Override
    public double getMean() {
      return toUnit(wrappedSnapshot.getMean());
    }

    @Override
    public long getMin() {
      return toUnit(wrappedSnapshot.getMin());
    }

    @Override
    public double getStdDev() {
      return toUnit(wrappedSnapshot.getStdDev());
    }

    @Override
    public void dump(OutputStream outputStream) {
      try (PrintWriter writer = new PrintWriter(outputStream)) {
        for (long value : getValues()) {
          writer.println(value);
        }
      }
    }
  }
}
