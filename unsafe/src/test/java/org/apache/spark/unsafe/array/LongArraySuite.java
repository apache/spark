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

package org.apache.spark.unsafe.array;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.unsafe.memory.MemoryBlock;

public class LongArraySuite {

  @Test
  public void basicTest() {
    long[] bytes = new long[2];
    LongArray arr = new LongArray(MemoryBlock.fromLongArray(bytes));
    arr.set(0, 1L);
    arr.set(1, 2L);
    arr.set(1, 3L);
    Assert.assertEquals(2, arr.size());
    Assert.assertEquals(1L, arr.get(0));
    Assert.assertEquals(3L, arr.get(1));

    arr.zeroOut();
    Assert.assertEquals(0L, arr.get(0));
    Assert.assertEquals(0L, arr.get(1));
  }
}
