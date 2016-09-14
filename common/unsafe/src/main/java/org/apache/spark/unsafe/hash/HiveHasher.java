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

package org.apache.spark.unsafe.hash;

import org.apache.spark.unsafe.Platform;

/**
 * Simulates Hive's hashing function at
 * org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils#hashcode()
 */
public class HiveHasher {
  private final long seed;

  public HiveHasher() {
    this.seed = 0;
  }

  @Override
  public String toString() {
    return "HiveHasher";
  }

  public int hashInt(int input) {
    return hashInt(input, seed);
  }

  public int hashLong(long input) {
    return hashLong(input, seed);
  }

  public int hashUnsafeBytes(Object base, long offset, int lengthInBytes) {
    return hashUnsafeBytes(base, offset, lengthInBytes, seed);
  }

  public static int hashInt(int input, long seed) {
    return input;
  }

  public static int hashLong(long input, long seed) {
    return (int) ((input >>> 32) ^ input);
  }

  public static int hashUnsafeBytes(Object base, long offset, int lengthInBytes, long seed) {
    assert (lengthInBytes >= 0): "lengthInBytes cannot be negative";
    int result = 0;
    for (int i = 0; i < lengthInBytes; i++) {
      result = (result * 31) + (int) Platform.getByte(base, offset + i);
    }
    return result;
  }
}
