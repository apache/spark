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

package org.apache.spark.util.collection.unsafe.sort;

import com.google.common.primitives.UnsignedLongs;

import org.apache.spark.annotation.Private;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.Utils;

@Private
public class PrefixComparators {
  private PrefixComparators() {}

  public static final StringPrefixComparator STRING = new StringPrefixComparator();
  public static final StringPrefixComparatorDesc STRING_DESC = new StringPrefixComparatorDesc();
  public static final BinaryPrefixComparator BINARY = new BinaryPrefixComparator();
  public static final BinaryPrefixComparatorDesc BINARY_DESC = new BinaryPrefixComparatorDesc();
  public static final LongPrefixComparator LONG = new LongPrefixComparator();
  public static final LongPrefixComparatorDesc LONG_DESC = new LongPrefixComparatorDesc();
  public static final DoublePrefixComparator DOUBLE = new DoublePrefixComparator();
  public static final DoublePrefixComparatorDesc DOUBLE_DESC = new DoublePrefixComparatorDesc();

  public static final class StringPrefixComparator extends PrefixComparator {
    @Override
    public int compare(long aPrefix, long bPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }

    public static long computePrefix(UTF8String value) {
      return value == null ? 0L : value.getPrefix();
    }
  }

  public static final class StringPrefixComparatorDesc extends PrefixComparator {
    @Override
    public int compare(long bPrefix, long aPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  public static final class BinaryPrefixComparator extends PrefixComparator {
    @Override
    public int compare(long aPrefix, long bPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }

    public static long computePrefix(byte[] bytes) {
      if (bytes == null) {
        return 0L;
      } else {
        /**
         * TODO: If a wrapper for BinaryType is created (SPARK-8786),
         * these codes below will be in the wrapper class.
         */
        final int minLen = Math.min(bytes.length, 8);
        long p = 0;
        for (int i = 0; i < minLen; ++i) {
          p |= (128L + Platform.getByte(bytes, Platform.BYTE_ARRAY_OFFSET + i))
              << (56 - 8 * i);
        }
        return p;
      }
    }
  }

  public static final class BinaryPrefixComparatorDesc extends PrefixComparator {
    @Override
    public int compare(long bPrefix, long aPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  public static final class LongPrefixComparator extends PrefixComparator {
    @Override
    public int compare(long a, long b) {
      return (a < b) ? -1 : (a > b) ? 1 : 0;
    }
  }

  public static final class LongPrefixComparatorDesc extends PrefixComparator {
    @Override
    public int compare(long b, long a) {
      return (a < b) ? -1 : (a > b) ? 1 : 0;
    }
  }

  public static final class DoublePrefixComparator extends PrefixComparator {
    @Override
    public int compare(long aPrefix, long bPrefix) {
      double a = Double.longBitsToDouble(aPrefix);
      double b = Double.longBitsToDouble(bPrefix);
      return Utils.nanSafeCompareDoubles(a, b);
    }

    public static long computePrefix(double value) {
      return Double.doubleToLongBits(value);
    }
  }

  public static final class DoublePrefixComparatorDesc extends PrefixComparator {
    @Override
    public int compare(long bPrefix, long aPrefix) {
      double a = Double.longBitsToDouble(aPrefix);
      double b = Double.longBitsToDouble(bPrefix);
      return Utils.nanSafeCompareDoubles(a, b);
    }

    public static long computePrefix(double value) {
      return Double.doubleToLongBits(value);
    }
  }
}
