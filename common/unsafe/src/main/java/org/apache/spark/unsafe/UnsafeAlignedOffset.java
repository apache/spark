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

/**
 * Class to make changes to record length offsets uniform through out
 * various areas of Apache Spark core and unsafe.  The SPARC platform
 * requires this because using a 4 byte Int for record lengths causes
 * the entire record of 8 byte Items to become misaligned by 4 bytes.
 * Using a 8 byte long for record length keeps things 8 byte aligned.
 */
public class UnsafeAlignedOffset {

  private static final int UAO_SIZE = Platform.unaligned() ? 4 : 8;

  private static int TEST_UAO_SIZE = 0;

  // used for test only
  public static void setUaoSize(int size) {
    assert size == 0 || size == 4 || size == 8;
    TEST_UAO_SIZE = size;
  }

  public static int getUaoSize() {
    return TEST_UAO_SIZE == 0 ? UAO_SIZE : TEST_UAO_SIZE;
  }

  public static int getSize(Object object, long offset) {
    return switch (getUaoSize()) {
      case 4 -> Platform.getInt(object, offset);
      case 8 -> (int) Platform.getLong(object, offset);
      default ->
        // checkstyle.off: RegexpSinglelineJava
        throw new AssertionError("Illegal UAO_SIZE");
        // checkstyle.on: RegexpSinglelineJava
    };
  }

  public static void putSize(Object object, long offset, int value) {
    switch (getUaoSize()) {
      case 4 -> Platform.putInt(object, offset, value);
      case 8 -> Platform.putLong(object, offset, value);
      default ->
        // checkstyle.off: RegexpSinglelineJava
        throw new AssertionError("Illegal UAO_SIZE");
        // checkstyle.on: RegexpSinglelineJava
    }
  }
}
