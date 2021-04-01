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

package org.apache.spark.remoteshuffle.testutil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class NettyMemoryExperiments {

  private static void testMemorySize() {
    long allocatedBytes = 0;
    long totalBytes = 4L * 1024 * 1024 * 1024;
    while (allocatedBytes < totalBytes) {
      int bufferSize = 4096;
      ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(bufferSize);
      allocatedBytes += bufferSize;
      System.out.println(String.format(
          "Allocated bytes: %s, Netty heap memory: %s, Netty directy memory: %s",
          allocatedBytes,
          PooledByteBufAllocator.DEFAULT.metric().usedHeapMemory(),
          PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory()));
      buf.release();
    }

  }

  public static void main(String[] args) {
    testMemorySize();
  }
}
