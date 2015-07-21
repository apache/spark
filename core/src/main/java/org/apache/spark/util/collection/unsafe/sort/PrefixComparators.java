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

import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;

import org.apache.spark.annotation.Private;
import org.apache.spark.unsafe.types.UTF8String;

@Private
public class PrefixComparators {
  private PrefixComparators() {}

  public static final StringPrefixComparator STRING = new StringPrefixComparator();
  public static final IntegralPrefixComparator INTEGRAL = new IntegralPrefixComparator();
  public static final FloatPrefixComparator FLOAT = new FloatPrefixComparator();
  public static final DoublePrefixComparator DOUBLE = new DoublePrefixComparator();

  public static final class StringPrefixComparator extends PrefixComparator {
    @Override
    public int compare(long aPrefix, long bPrefix) {
      // TODO: can done more efficiently
      byte[] a = Longs.toByteArray(aPrefix);
      byte[] b = Longs.toByteArray(bPrefix);
      for (int i = 0; i < 8; i++) {
        int c = UnsignedBytes.compare(a[i], b[i]);
        if (c != 0) return c;
      }
      return 0;
    }

    public long computePrefix(byte[] bytes) {
      if (bytes == null) {
        return 0L;
      } else {
        byte[] padded = new byte[8];
        System.arraycopy(bytes, 0, padded, 0, Math.min(bytes.length, 8));
        return Longs.fromByteArray(padded);
      }
    }

    public long computePrefix(String value) {
      return value == null ? 0L : computePrefix(value.getBytes(Charsets.UTF_8));
    }

    public long computePrefix(UTF8String value) {
      return value == null ? 0L : computePrefix(value.getBytes());
    }
  }

  /**
   * Prefix comparator for all integral types (boolean, byte, short, int, long).
   */
  public static final class IntegralPrefixComparator extends PrefixComparator {
    @Override
    public int compare(long a, long b) {
      return (a < b) ? -1 : (a > b) ? 1 : 0;
    }

    public final long NULL_PREFIX = Long.MIN_VALUE;
  }

  public static final class FloatPrefixComparator extends PrefixComparator {
    @Override
    public int compare(long aPrefix, long bPrefix) {
      float a = Float.intBitsToFloat((int) aPrefix);
      float b = Float.intBitsToFloat((int) bPrefix);
      return (a < b) ? -1 : (a > b) ? 1 : 0;
    }

    public long computePrefix(float value) {
      return Float.floatToIntBits(value) & 0xffffffffL;
    }

    public final long NULL_PREFIX = computePrefix(Float.NEGATIVE_INFINITY);
  }

  public static final class DoublePrefixComparator extends PrefixComparator {
    @Override
    public int compare(long aPrefix, long bPrefix) {
      double a = Double.longBitsToDouble(aPrefix);
      double b = Double.longBitsToDouble(bPrefix);
      return (a < b) ? -1 : (a > b) ? 1 : 0;
    }

    public long computePrefix(double value) {
      return Double.doubleToLongBits(value);
    }

    public final long NULL_PREFIX = computePrefix(Double.NEGATIVE_INFINITY);
  }
}
