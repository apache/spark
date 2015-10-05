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

package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.types.MapData;

/**
 * An Unsafe implementation of Map which is backed by raw memory instead of Java objects.
 *
 * Currently we just use 2 UnsafeArrayData to represent UnsafeMapData.
 *
 * Note that when we write out this map, we should write out the `numElements` at first 4 bytes,
 * and numBytes of key array at second 4 bytes, then follows key array content and value array
 * content without `numElements` header.
 * When we read in a map, we should read first 4 bytes as `numElements` and second 4 bytes as
 * numBytes of key array, and construct unsafe key array and value array with these 2 information.
 */
public class UnsafeMapData extends MapData {

  private final UnsafeArrayData keys;
  private final UnsafeArrayData values;
  // The number of elements in this array
  private int numElements;
  // The size of this array's backing data, in bytes
  private int sizeInBytes;

  public int getSizeInBytes() { return sizeInBytes; }

  public UnsafeMapData(UnsafeArrayData keys, UnsafeArrayData values) {
    assert keys.numElements() == values.numElements();
    this.sizeInBytes = keys.getSizeInBytes() + values.getSizeInBytes();
    this.numElements = keys.numElements();
    this.keys = keys;
    this.values = values;
  }

  @Override
  public int numElements() {
    return numElements;
  }

  @Override
  public UnsafeArrayData keyArray() {
    return keys;
  }

  @Override
  public UnsafeArrayData valueArray() {
    return values;
  }

  @Override
  public UnsafeMapData copy() {
    return new UnsafeMapData(keys.copy(), values.copy());
  }
}
