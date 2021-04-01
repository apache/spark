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

package org.apache.spark.remoteshuffle.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RateCounterTest {

  @Test
  public void addValueAndGetRate() throws InterruptedException {
    long checkpointMillis = 10;
    long startTime = System.currentTimeMillis();

    RateCounter rateCounter = new RateCounter(checkpointMillis);

    Double rate = rateCounter.addValueAndGetRate(20);
    if (System.currentTimeMillis() - startTime < checkpointMillis) {
      Assert.assertNull(rate);
    }

    Thread.sleep(checkpointMillis);
    rate = rateCounter.addValueAndGetRate(20);
    Assert.assertNotNull(rate);
    Assert.assertTrue(rate > 0);

    Assert.assertTrue(rateCounter.getOverallRate() > 0);
  }

  @Test
  public void checkRateAccuracy() throws InterruptedException {
    long checkpointMillis = 10;

    RateCounter rateCounter = new RateCounter(checkpointMillis);

    Thread.sleep(checkpointMillis);

    Double rate = rateCounter.addValueAndGetRate(100 * checkpointMillis);

    // rate should be roughly between 10 and 1000
    Assert.assertTrue(rate > 10);
    Assert.assertTrue(rate < 1000);
  }
}
