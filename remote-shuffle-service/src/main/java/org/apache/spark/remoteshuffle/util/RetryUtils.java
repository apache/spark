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

import org.apache.spark.remoteshuffle.exceptions.RssInvalidStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class RetryUtils {
  private static final Logger logger = LoggerFactory.getLogger(RetryUtils.class);

  /***
   * Retry the callable until it returns true.
   * @param intervalMillis
   * @param maxTryMillis
   * @param callable
   * @return
   */
  public static boolean retryUntilTrue(long intervalMillis, long maxTryMillis,
                                       Supplier<Boolean> callable) {
    long startMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() - startMillis <= maxTryMillis) {
      if (callable.get()) {
        return true;
      }
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException e) {
        logger.warn("Interrupted when waiting in retry", e);
      }
    }
    return false;
  }

  /***
   * Retry the callable until it returns not null.
   * @param intervalMillis
   * @param maxTryMillis
   * @param callable
   * @return
   */
  public static <T> T retryUntilNotNull(long intervalMillis, long maxTryMillis,
                                        Supplier<T> callable) {
    long startMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() - startMillis <= maxTryMillis) {
      T result = callable.get();
      if (result != null) {
        return result;
      }
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException e) {
        logger.warn("Interrupted when waiting in retry", e);
      }
    }
    return null;
  }

  /***
   * Retry the callable until it returns not null.
   * @param minIntervalMillis
   * @param maxIntervalMillis
   * @param maxTryMillis
   * @param callable
   * @return
   */
  public static <T> T retryUntilNotNull(long minIntervalMillis, long maxIntervalMillis,
                                        long maxTryMillis, Supplier<T> callable) {
    final long startMillis = System.currentTimeMillis();
    long intervalMillis = minIntervalMillis;
    while (System.currentTimeMillis() - startMillis <= maxTryMillis) {
      T result = callable.get();
      if (result != null) {
        return result;
      }
      long sleepMillis = intervalMillis;
      long remainMillis = maxTryMillis - (System.currentTimeMillis() - startMillis);
      if (sleepMillis >= remainMillis) {
        sleepMillis = remainMillis;
      }
      if (sleepMillis > 0) {
        ThreadUtils.sleep(sleepMillis);
      }

      intervalMillis *= 2;
      if (intervalMillis > maxIntervalMillis) {
        intervalMillis = maxIntervalMillis;
      }
    }
    return null;
  }

  /***
   * Retry the callable until it succeeds.
   * @return
   */
  public static <T> T retry(long minIntervalMillis, long maxIntervalMillis, long retryMaxMillis,
                            String logInfo, Supplier<T> callable) {
    long intervalMillis = minIntervalMillis;
    long startTime = System.currentTimeMillis();
    int triedTimes = 0;
    Throwable lastException;
    do {
      try {
        triedTimes++;
        return callable.get();
      } catch (Throwable ex) {
        lastException = ex;
        String str = String.format(
            "Failed (tried %s times and %s milliseconds, max retry milliseconds: %s) to execute: %s",
            triedTimes, System.currentTimeMillis() - startTime, retryMaxMillis, logInfo);
        logger.warn(str, ex);
        long retryRemainingMillis = startTime + retryMaxMillis - System.currentTimeMillis();
        if (retryRemainingMillis <= 0) {
          break;
        } else {
          long waitMillis = Math.min(intervalMillis, retryRemainingMillis);
          logger.info(String.format(
              "Waiting %s milliseconds (remaining milliseconds: %s) and executing again: %s",
              waitMillis, retryRemainingMillis, logInfo));
          ThreadUtils.sleep(waitMillis);
          intervalMillis *= 2;
          if (intervalMillis > maxIntervalMillis) {
            intervalMillis = maxIntervalMillis;
          }
        }
        continue;
      }
    } while (System.currentTimeMillis() <= startTime + retryMaxMillis);

    ExceptionUtils.throwException(lastException);

    throw new RssInvalidStateException(
        "Should not run into here because the previous line of code will throw out exception!");
  }
}
