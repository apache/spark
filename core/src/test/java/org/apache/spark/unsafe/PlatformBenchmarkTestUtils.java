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

package org.apache.spark.unsafe;

public class PlatformBenchmarkTestUtils {

  /**
   * Copies memory from the source (either on-heap or off-heap) to the destination integer array
   * using a manual loop. This method mimics the implementation of manual memory copying found
   * in {@link org.apache.spark.sql.execution.vectorized.OnHeapColumnVector}.
   *
   * @param src The source object (null for off-heap memory).
   * @param srcOffset The initial offset in the source memory.
   * @param dst The destination integer array.
   * @param count The number of integers to copy.
   */
  public static void copyToIntArrayManual(Object src, long srcOffset, int[] dst, int count) {
    for (int i = 0; i < count; ++i, srcOffset += 4) {
      dst[i] = Platform.getInt(src, srcOffset);
    }
  }
}
