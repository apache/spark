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

import org.testng.Assert;
import org.testng.annotations.Test;

public class MovingAverageCalculatorTest {

  @Test
  public void singleValueCapacity() {
    MovingAverageCalculator calculator = new MovingAverageCalculator(1);
    Assert.assertEquals(calculator.getAverage(), 0L);

    calculator.addValue(1);
    Assert.assertEquals(calculator.getAverage(), 1L);

    calculator.addValue(0);
    Assert.assertEquals(calculator.getAverage(), 0L);

    calculator.addValue(100);
    Assert.assertEquals(calculator.getAverage(), 100L);
  }

  @Test
  public void twoValuesCapacity() {
    MovingAverageCalculator calculator = new MovingAverageCalculator(2);
    Assert.assertEquals(calculator.getAverage(), 0L);

    calculator.addValue(1);
    Assert.assertEquals(calculator.getAverage(), 1L);

    calculator.addValue(3);
    Assert.assertEquals(calculator.getAverage(), 2L);

    calculator.addValue(5);
    Assert.assertEquals(calculator.getAverage(), 4L);
  }

  @Test
  public void threeValuesCapacity() {
    MovingAverageCalculator calculator = new MovingAverageCalculator(3);
    Assert.assertEquals(calculator.getAverage(), 0L);

    calculator.addValue(1);
    Assert.assertEquals(calculator.getAverage(), 1L);

    calculator.addValue(3);
    Assert.assertEquals(calculator.getAverage(), 2L);

    calculator.addValue(5);
    Assert.assertEquals(calculator.getAverage(), 3L);

    calculator.addValue(2);
    Assert.assertEquals(calculator.getAverage(), 3L);

    calculator.addValue(4);
    Assert.assertEquals(calculator.getAverage(), 4L);
  }

  @Test
  public void multiThreading() throws InterruptedException {
    MovingAverageCalculator calculator = new MovingAverageCalculator(11);

    Thread[] threads = new Thread[1000];

    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        for (int repeat = 0; repeat < 10000; repeat++) {
          calculator.addValue(7);
        }
      });
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    Assert.assertEquals(calculator.getAverage(), 7L);
  }
}
