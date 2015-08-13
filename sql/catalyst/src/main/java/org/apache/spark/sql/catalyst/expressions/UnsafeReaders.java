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

import org.apache.spark.unsafe.Platform;

public class UnsafeReaders {

  public static UnsafeArrayData readArray(Object baseObject, long baseOffset, int numBytes) {
    // Read the number of elements from first 4 bytes.
    final int numElements = Platform.getInt(baseObject, baseOffset);
    final UnsafeArrayData array = new UnsafeArrayData();
    // Skip the first 4 bytes.
    array.pointTo(baseObject, baseOffset + 4, numElements, numBytes - 4);
    return array;
  }

  public static UnsafeMapData readMap(Object baseObject, long baseOffset, int numBytes) {
    // Read the number of elements from first 4 bytes.
    final int numElements = Platform.getInt(baseObject, baseOffset);
    // Read the numBytes of key array in second 4 bytes.
    final int keyArraySize = Platform.getInt(baseObject, baseOffset + 4);
    final int valueArraySize = numBytes - 8 - keyArraySize;

    final UnsafeArrayData keyArray = new UnsafeArrayData();
    keyArray.pointTo(baseObject, baseOffset + 8, numElements, keyArraySize);

    final UnsafeArrayData valueArray = new UnsafeArrayData();
    valueArray.pointTo(baseObject, baseOffset + 8 + keyArraySize, numElements, valueArraySize);

    return new UnsafeMapData(keyArray, valueArray);
  }
}
