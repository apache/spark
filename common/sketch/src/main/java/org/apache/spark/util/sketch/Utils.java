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

package org.apache.spark.util.sketch;

import java.nio.charset.StandardCharsets;

class Utils {
  public static byte[] getBytesFromUTF8String(String str) {
    return str.getBytes(StandardCharsets.UTF_8);
  }

  public static long integralToLong(Object i) {
    long longValue;

    if (i instanceof Long longVal) {
      longValue = longVal;
    } else if (i instanceof Integer integer) {
      longValue = integer.longValue();
    } else if (i instanceof Short shortVal) {
      longValue = shortVal.longValue();
    } else if (i instanceof Byte byteVal) {
      longValue = byteVal.longValue();
    } else {
      throw new IllegalArgumentException("Unsupported data type " + i.getClass().getName());
    }

    return longValue;
  }
}
