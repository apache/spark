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

package org.apache.spark.remoteshuffle.tools;

import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tool repeatedly runs StreamServerStressTool with writing 64GB data which exceeds max integer value.
 */
public class StreamServerStressToolWrite64GB {
  private static final Logger logger =
      LoggerFactory.getLogger(StreamServerStressToolWrite64GB.class);

  public static void main(String[] args) {
    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        logger.error(String.format("Got exception from thread %s", t.getName()), e);
        System.exit(-1);
      }
    });

    StreamServerStressTool tool = new StreamServerStressTool();
    tool.setNumBytes(64L * 1024 * 1024 * 1024);
    tool.setNumMaps(2);
    tool.setNumMapAttempts(1);
    tool.setNumPartitions(2);
    tool.setNumSplits(1);
    tool.setWriteClientQueueSize(1000);

    try {
      tool.run();
    } finally {
      tool.cleanup();
    }

    M3Stats.closeDefaultScope();
  }
}
