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

package org.apache.spark.network.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkPooledByteBufAllocator extends PooledByteBufAllocator {
  private final Logger LOG = LoggerFactory.getLogger(SparkPooledByteBufAllocator.class);

  public SparkPooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena,
      int pageSize, int maxOrder, int tinyCacheSize, int smallCacheSize, int normalCacheSize,
      boolean useCacheForAllThreads) {
    super(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder, tinyCacheSize,
        smallCacheSize, normalCacheSize, useCacheForAllThreads);
  }

  @Override
  protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
    if (initialCapacity > 1024 * 1024) {
      LOG.info("Try to allocate direct buffer with initialCapacity: " +
          initialCapacity + " maxCapacity: " + maxCapacity + ", current usedDirectMemory: " +
          metric().usedDirectMemory());
    }
    return super.newDirectBuffer(initialCapacity, maxCapacity);
  }

  @Override
  protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
    if (initialCapacity > 1024 * 1024) {
      LOG.info("Try to allocate heap buffer with initialCapacity: " +
          initialCapacity + " maxCapacity: " + maxCapacity + ", current usedHeapMemory: " +
          metric().usedHeapMemory());
    }
    return super.newHeapBuffer(initialCapacity, maxCapacity);
  }
}
