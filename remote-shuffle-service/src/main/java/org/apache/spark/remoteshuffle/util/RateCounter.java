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

import java.util.concurrent.atomic.AtomicLong;

/***
 * This class wraps the logic to track a counter and its value change rate.
 */
public class RateCounter {
  private final AtomicLong totalValue = new AtomicLong();
  private final AtomicLong checkpointValue = new AtomicLong();
  private final AtomicLong checkpointTime = new AtomicLong();

  private final long checkpointMillis;

  private final long overallStartTime;

  /***
   * Creates an instance.
   * @param checkpointMillis this is the checkpoint interval. When time elapses longer than
   *                         this interval, we will calculate the value change rate.
   */
  public RateCounter(long checkpointMillis) {
    if (checkpointMillis <= 0) {
      throw new RuntimeException("Invalid value for checkpointMillis: " + checkpointMillis);
    }
    this.checkpointMillis = checkpointMillis;

    this.overallStartTime = System.currentTimeMillis();
    this.checkpointTime.set(this.overallStartTime);
  }

  /***
   * Add value, and return the rate if time has elapsed longer than checkpoint interval.
   * @param delta
   * @return
   */
  public Double addValueAndGetRate(long delta) {
    long newValue = totalValue.addAndGet(delta);

    long currentTime = System.currentTimeMillis();
    long lastCheckpointTime = checkpointTime.get();
    if (currentTime - lastCheckpointTime >= checkpointMillis) {
      checkpointTime.set(currentTime);
      long prevCheckpointValue = checkpointValue.getAndSet(newValue);
      double ratePerMillis =
          (double) (newValue - prevCheckpointValue) / (double) (currentTime - lastCheckpointTime);
      return ratePerMillis;
    }

    return null;
  }

  /***
   * Get overall value.
   * @return
   */
  public long getOverallValue() {
    return totalValue.get();
  }

  /***
   * Get overall change rate since this instance is created.
   * @return
   */
  public double getOverallRate() {
    long duration = System.currentTimeMillis() - overallStartTime;
    if (duration == 0) {
      return 0.0;
    }

    double ratePerMillis = (double) totalValue.get() / (double) duration;
    return ratePerMillis;
  }
}
