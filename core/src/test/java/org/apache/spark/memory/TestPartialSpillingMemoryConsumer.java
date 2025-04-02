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

package org.apache.spark.memory;

import java.io.IOException;

/**
 * A TestMemoryConsumer which, when asked to spill, releases only enough memory to satisfy the
 * request rather than releasing all its memory.
 */
public class TestPartialSpillingMemoryConsumer extends TestMemoryConsumer {
  private long spilledBytes = 0L;

  public TestPartialSpillingMemoryConsumer(TaskMemoryManager memoryManager, MemoryMode mode) {
    super(memoryManager, mode);
  }
  public TestPartialSpillingMemoryConsumer(TaskMemoryManager memoryManager) {
    super(memoryManager);
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    long used = getUsed();
    long released = Math.min(used, size);
    free(released);
    spilledBytes += released;
    return released;
  }

  public long getSpilledBytes() {
    return spilledBytes;
  }
}
