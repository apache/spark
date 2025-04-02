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

@Private
public class PrefixComparators {
  private PrefixComparators() {}

  public static final PrefixComparator STRING = new UnsignedPrefixComparator();
  public static final PrefixComparator STRING_DESC = new UnsignedPrefixComparatorDesc();
  public static final PrefixComparator STRING_NULLS_LAST = new UnsignedPrefixComparatorNullsLast();
  public static final PrefixComparator STRING_DESC_NULLS_FIRST =
    new UnsignedPrefixComparatorDescNullsFirst();

  public static final PrefixComparator BINARY = new UnsignedPrefixComparator();
  public static final PrefixComparator BINARY_DESC = new UnsignedPrefixComparatorDesc();
  public static final PrefixComparator BINARY_NULLS_LAST = new UnsignedPrefixComparatorNullsLast();
  public static final PrefixComparator BINARY_DESC_NULLS_FIRST =
    new UnsignedPrefixComparatorDescNullsFirst();

  public static final PrefixComparator LONG = new SignedPrefixComparator();
  public static final PrefixComparator LONG_DESC = new SignedPrefixComparatorDesc();
  public static final PrefixComparator LONG_NULLS_LAST = new SignedPrefixComparatorNullsLast();
  public static final PrefixComparator LONG_DESC_NULLS_FIRST =
    new SignedPrefixComparatorDescNullsFirst();

  public static final PrefixComparator DOUBLE = new UnsignedPrefixComparator();
  public static final PrefixComparator DOUBLE_DESC = new UnsignedPrefixComparatorDesc();
  public static final PrefixComparator DOUBLE_NULLS_LAST = new UnsignedPrefixComparatorNullsLast();
  public static final PrefixComparator DOUBLE_DESC_NULLS_FIRST =
    new UnsignedPrefixComparatorDescNullsFirst();

  public static final class StringPrefixComparator {
    public static long computePrefix(UTF8String value) {
      return value == null ? 0L : value.getPrefix();
    }
  }

  public static final class BinaryPrefixComparator {
    public static long computePrefix(byte[] bytes) {
      return ByteArray.getPrefix(bytes);
    }
  }

  public static final class DoublePrefixComparator {
    /**
     * Converts the double into a value that compares correctly as an unsigned long. For more
     * details see http://stereopsis.com/radix.html.
     */
    public static long computePrefix(double value) {
      // normalize -0.0 to 0.0, as they should be equal
      value = value == -0.0 ? 0.0 : value;
      // Java's doubleToLongBits already canonicalizes all NaN values to the smallest possible
      // positive NaN, so there's nothing special we need to do for NaNs.
      long bits = Double.doubleToLongBits(value);
      // Negative floats compare backwards due to their sign-magnitude representation, so flip
      // all the bits in this case.
      long mask = -(bits >>> 63) | 0x8000000000000000L;
      return bits ^ mask;
    }
  }

  /**
   * Provides radix sort parameters. Comparators implementing this also are indicating that the
   * ordering they define is compatible with radix sort.
   */
  public abstract static class RadixSortSupport extends PrefixComparator {
    /** @return Whether the sort should be descending in binary sort order. */
    public abstract boolean sortDescending();

    /** @return Whether the sort should take into account the sign bit. */
    public abstract boolean sortSigned();

    /** @return Whether the sort should put nulls first or last. */
    public abstract boolean nullsFirst();
  }

  //
  // Standard prefix comparator implementations
  //

  public static final class UnsignedPrefixComparator extends RadixSortSupport {
    @Override public boolean sortDescending() { return false; }
    @Override public boolean sortSigned() { return false; }
    @Override public boolean nullsFirst() { return true; }
    @Override
    public int compare(long aPrefix, long bPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  public static final class UnsignedPrefixComparatorNullsLast extends RadixSortSupport {
    @Override public boolean sortDescending() { return false; }
    @Override public boolean sortSigned() { return false; }
    @Override public boolean nullsFirst() { return false; }
    @Override
    public int compare(long aPrefix, long bPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  public static final class UnsignedPrefixComparatorDescNullsFirst extends RadixSortSupport {
    @Override public boolean sortDescending() { return true; }
    @Override public boolean sortSigned() { return false; }
    @Override public boolean nullsFirst() { return true; }
    @Override
    public int compare(long bPrefix, long aPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  public static final class UnsignedPrefixComparatorDesc extends RadixSortSupport {
    @Override public boolean sortDescending() { return true; }
    @Override public boolean sortSigned() { return false; }
    @Override public boolean nullsFirst() { return false; }
    @Override
    public int compare(long bPrefix, long aPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  public static final class SignedPrefixComparator extends RadixSortSupport {
    @Override public boolean sortDescending() { return false; }
    @Override public boolean sortSigned() { return true; }
    @Override public boolean nullsFirst() { return true; }
    @Override
    public int compare(long a, long b) {
      return Long.compare(a, b);
    }
  }

  public static final class SignedPrefixComparatorNullsLast extends RadixSortSupport {
    @Override public boolean sortDescending() { return false; }
    @Override public boolean sortSigned() { return true; }
    @Override public boolean nullsFirst() { return false; }
    @Override
    public int compare(long a, long b) {
      return Long.compare(a, b);
    }
  }

  public static final class SignedPrefixComparatorDescNullsFirst extends RadixSortSupport {
    @Override public boolean sortDescending() { return true; }
    @Override public boolean sortSigned() { return true; }
    @Override public boolean nullsFirst() { return true; }
    @Override
    public int compare(long b, long a) {
      return Long.compare(a, b);
    }
  }

  public static final class SignedPrefixComparatorDesc extends RadixSortSupport {
    @Override public boolean sortDescending() { return true; }
    @Override public boolean sortSigned() { return true; }
    @Override public boolean nullsFirst() { return false; }
    @Override
    public int compare(long b, long a) {
      return Long.compare(a, b);
    }
  }
}
