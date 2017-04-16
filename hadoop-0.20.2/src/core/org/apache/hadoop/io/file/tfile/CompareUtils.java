/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io.file.tfile;

import java.util.Comparator;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

class CompareUtils {
  /**
   * Prevent the instantiation of class.
   */
  private CompareUtils() {
    // nothing
  }

  /**
   * A comparator to compare anything that implements {@link RawComparable}
   * using a customized comparator.
   */
  public static final class BytesComparator implements
      Comparator<RawComparable> {
    private RawComparator<Object> cmp;

    public BytesComparator(RawComparator<Object> cmp) {
      this.cmp = cmp;
    }

    @Override
    public int compare(RawComparable o1, RawComparable o2) {
      return compare(o1.buffer(), o1.offset(), o1.size(), o2.buffer(), o2
          .offset(), o2.size());
    }

    public int compare(byte[] a, int off1, int len1, byte[] b, int off2,
        int len2) {
      return cmp.compare(a, off1, len1, b, off2, len2);
    }
  }

  /**
   * Interface for all objects that has a single integer magnitude.
   */
  static interface Scalar {
    long magnitude();
  }

  static final class ScalarLong implements Scalar {
    private long magnitude;

    public ScalarLong(long m) {
      magnitude = m;
    }

    public long magnitude() {
      return magnitude;
    }
  }

  public static final class ScalarComparator implements Comparator<Scalar> {
    @Override
    public int compare(Scalar o1, Scalar o2) {
      long diff = o1.magnitude() - o2.magnitude();
      if (diff < 0) return -1;
      if (diff > 0) return 1;
      return 0;
    }
  }

  public static final class MemcmpRawComparator implements
      RawComparator<Object> {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
    }

    @Override
    public int compare(Object o1, Object o2) {
      throw new RuntimeException("Object comparison not supported");
    }
  }
}
