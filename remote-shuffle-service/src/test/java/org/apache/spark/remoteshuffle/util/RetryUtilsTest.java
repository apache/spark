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

import java.util.concurrent.atomic.AtomicInteger;

public class RetryUtilsTest {

  @Test
  public void firstTrySucceeds() {
    String result = RetryUtils.retry(100, 100, 0, "test", () -> {
      return "ok";
    });
    Assert.assertEquals(result, "ok");
  }

  @Test
  public void multipleRetry() {
    int retryMaxMillis = 100;
    AtomicInteger counter = new AtomicInteger();
    long startTime = System.currentTimeMillis();
    RetryUtils.retry(0, 60000, retryMaxMillis, "test", () -> {
      int value = counter.incrementAndGet();
      if (System.currentTimeMillis() - startTime <= retryMaxMillis / 2) {
        throw new RuntimeException("simulated exception");
      }
      return value;
    });
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testThrowException() {
    int retryMaxMillis = 100;
    AtomicInteger counter = new AtomicInteger();
    RetryUtils.retry(1, 10, retryMaxMillis, "test", () -> {
      counter.incrementAndGet();
      throw new RuntimeException("simulated exception");
    });
  }

}
