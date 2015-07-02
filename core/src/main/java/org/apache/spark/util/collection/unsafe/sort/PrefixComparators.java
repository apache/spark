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

import org.apache.spark.annotation.Private;

@Private
public class PrefixComparators {
  private PrefixComparators() {}

  public static final IntPrefixComparator INTEGER = new IntPrefixComparator();
  public static final LongPrefixComparator LONG = new LongPrefixComparator();
  public static final FloatPrefixComparator FLOAT = new FloatPrefixComparator();
  public static final DoublePrefixComparator DOUBLE = new DoublePrefixComparator();

  public static final class IntPrefixComparator extends PrefixComparator {
    @Override
    public int compare(long aPrefix, long bPrefix) {
      int a = (int) aPrefix;
      int b = (int) bPrefix;
      return (a < b) ? -1 : (a > b) ? 1 : 0;
    }

    public long computePrefix(int value) {
      return value & 0xffffffffL;
    }
  }

  public static final class LongPrefixComparator extends PrefixComparator {
    @Override
    public int compare(long a, long b) {
      return (a < b) ? -1 : (a > b) ? 1 : 0;
    }
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
  }
}
