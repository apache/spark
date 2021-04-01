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

package org.apache.spark.remoteshuffle.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/***
 * This class wraps the logic to calculate moving average of given values.
 * This class is thread safe.
 */
public class MovingAverageCalculator {
  private final AtomicLongArray values;
  private AtomicInteger index = new AtomicInteger(0);
  private volatile boolean fullyFilled = false;

  public MovingAverageCalculator(int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException(
          "capacity should be larger than 0, but has value: " + capacity);
    }
    this.values = new AtomicLongArray(capacity);
  }

  public void addValue(long v) {
    int currentIndex = index.getAndIncrement();
    if (currentIndex >= values.length()) {
      fullyFilled = true;
    }
    currentIndex = currentIndex % values.length();
    values.set(currentIndex, v);
  }

  public long getAverage() {
    int endIndexExclusive = fullyFilled ? values.length() : index.get();
    if (endIndexExclusive <= 0) {
      return 0;
    }

    long sum = 0;
    for (int i = 0; i < endIndexExclusive; i++) {
      sum += values.get(i);
    }
    return Math.round((double) sum / (double) endIndexExclusive);
  }
}
