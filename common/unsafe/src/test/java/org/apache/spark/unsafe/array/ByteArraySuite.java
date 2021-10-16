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

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.ByteArray;
import org.junit.Assert;
import org.junit.Test;

public class ByteArraySuite {
  private long getPrefixByByte(byte[] bytes) {
    final int minLen = Math.min(bytes.length, 8);
    long p = 0;
    for (int i = 0; i < minLen; ++i) {
      p |= ((long) Platform.getByte(bytes, Platform.BYTE_ARRAY_OFFSET + i) & 0xff)
              << (56 - 8 * i);
    }
    return p;
  }

  @Test
  public void testGetPrefix() {
    for (int i = 0; i <= 9; i++) {
      byte[] bytes = new byte[i];
      int prefix = i - 1;
      while (prefix >= 0) {
        bytes[prefix] = (byte) prefix;
        prefix -= 1;
      }

      long result = ByteArray.getPrefix(bytes);
      long expected = getPrefixByByte(bytes);
      Assert.assertEquals(result, expected);
    }
  }
}
