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
import org.apache.spark.unsafe.types.ByteArray;
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

  //
  // Public classes
  //

  public static final class StringPrefixComparator extends UnsignedBinaryPrefixComparator {
    public static long computePrefix(UTF8String value) {
      return value == null ? 0L : value.getPrefix();
    }
  }

  public static final class StringPrefixComparatorDesc extends UnsignedBinaryPrefixComparatorDesc {
  }

  public static final class BinaryPrefixComparator extends UnsignedBinaryPrefixComparator {
    public static long computePrefix(byte[] bytes) {
      return ByteArray.getPrefix(bytes);
    }
  }

  public static final class BinaryPrefixComparatorDesc extends UnsignedBinaryPrefixComparatorDesc {
  }

  public static final class LongPrefixComparator extends TwosComplementPrefixComparator {
  }

  public static final class LongPrefixComparatorDesc extends TwosComplementPrefixComparatorDesc {
  }

  public static final class DoublePrefixComparator extends TwosComplementPrefixComparator {
    public static long computePrefix(double value) {
      // Java's doubleToLongBits already canonicalizes all NaN values to the lowest possible NaN,
      // so there's nothing special we need to do here.
      return Double.doubleToLongBits(value);
    }
  }

  public static final class DoublePrefixComparatorDesc extends TwosComplementPrefixComparator {
  }

  //
  // Abstract base classes for prefix comparators. These classes signal to sorters whether
  // (and how) radix sort can be used instead of comparison-based sorts.
  //

  private static abstract class UnsignedBinaryPrefixComparator extends PrefixComparator {
    @Override
    public final int compare(long aPrefix, long bPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  private static abstract class UnsignedBinaryPrefixComparatorDesc extends PrefixComparator {
    @Override
    public final int compare(long bPrefix, long aPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  private static abstract class TwosComplementPrefixComparator extends PrefixComparator {
    @Override
    public final int compare(long a, long b) {
      return (a < b) ? -1 : (a > b) ? 1 : 0;
    }
  }

  private static abstract class TwosComplementPrefixComparatorDesc extends PrefixComparator {
    @Override
    public final int compare(long b, long a) {
      return (a < b) ? -1 : (a > b) ? 1 : 0;
    }
  }
}
