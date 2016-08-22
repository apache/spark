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

import java.util.Arrays;

/**
 * Class to make changes to record length offsets uniform through out
 * various areas of Apache Spark core and unsafe.  The SPARC platform
 * requires this because using a 4 byte Int for record lengths causes
 * the entire record of 8 byte Items to become misaligned by 4 bytes.
 * Using a 8 byte long for record length keeps things 8 byte aligned.
 */
public class UnsafeAlignedOffset {

  private static final int UAO_SIZE;
  private static final boolean ALIGNED_ARCH = 
    Arrays.asList("sparc", "sparcv9").contains(System.getProperty("os.arch"));

  static {
    if (ALIGNED_ARCH) {
      UAO_SIZE = 8;
    } else {
      UAO_SIZE = 4;
    }
  }

  public static int getUaoSize() {
    return UAO_SIZE;
  }

  public static boolean getAlignedArch() {
    return ALIGNED_ARCH;
  }

  public static int getSize(Object object, long offset) {
    switch (UAO_SIZE) {
      case 4:
        return Platform.getInt(object, offset);
      case 8:
        return (int)Platform.getLong(object, offset);
      default:
        throw new AssertionError("Illegal UAO_SIZE");
    }
  }

  public static void putSize(Object object, long offset, int value) {
    switch (UAO_SIZE) {
      case 4:
        Platform.putInt(object, offset, value);
        break;
      case 8:
        Platform.putLong(object, offset, value);
        break;
      default:
        throw new AssertionError("Illegal UAO_SIZE");
    }
  }
}
