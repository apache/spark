/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Random algorithms.
 */
public class RandomAlgorithms {
  
  private interface IndexMapper {
    int get(int pos);
    void swap(int a, int b);
    int getSize();
    void reset();
  }

  /**
   * A sparse index mapping table - useful when we want to
   * non-destructively permute a small fraction of a large array.
   */
  private static class SparseIndexMapper implements IndexMapper {
    Map<Integer, Integer> mapping = new HashMap<Integer, Integer>();
    int size;
    
    SparseIndexMapper(int size) { 
      this.size = size;
    }
    
    public int get(int pos) {
      Integer mapped = mapping.get(pos);
      if (mapped == null) return pos;
      return mapped;
    }

    public void swap(int a, int b) {
      if (a == b) return;
      int valA = get(a);
      int valB = get(b);
      if (b == valA) {
        mapping.remove(b);
      } else {
        mapping.put(b, valA);
      }
      if (a == valB) {
        mapping.remove(a);
      } else {
        mapping.put(a, valB);
      }
    }
    
    public int getSize() {
      return size;
    }
    
    public void reset() {
      mapping.clear();
    }
  }

  /**
   * A dense index mapping table - useful when we want to
   * non-destructively permute a large fraction of an array.
   */
  private static class DenseIndexMapper implements IndexMapper {
    int[] mapping;

    DenseIndexMapper(int size) {
      mapping = new int[size];
      for (int i=0; i<size; ++i) {
        mapping[i] = i;
      }
    }

    public int get(int pos) {
      if ( (pos < 0) || (pos>=mapping.length) ) {
        throw new IndexOutOfBoundsException();
      }
      return mapping[pos];
    }

    public void swap(int a, int b) {
      if (a == b) return;
      int valA = get(a);
      int valB = get(b);
      mapping[a]=valB;
      mapping[b]=valA;
    }
    
    public int getSize() {
      return mapping.length;
    }
    
    public void reset() {
      return;
    }
  }

  /**
   * Iteratively pick random numbers from pool 0..n-1. Each number can only be
   * picked once.
   */
  public static class Selector {
    private IndexMapper mapping;
    private int n;
    private Random rand;

    /**
     * Constructor.
     * 
     * @param n
     *          The pool of integers: 0..n-1.
     * @param selPcnt
     *          Percentage of selected numbers. This is just a hint for internal
     *          memory optimization.
     * @param rand
     *          Random number generator.
     */
    public Selector(int n, double selPcnt, Random rand) {
      if (n <= 0) {
        throw new IllegalArgumentException("n should be positive");
      }
      
      boolean sparse = (n > 200) && (selPcnt < 0.1);
      
      this.n = n;
      mapping = (sparse) ? new SparseIndexMapper(n) : new DenseIndexMapper(n);
      this.rand = rand;
    }
    
    /**
     * Select the next random number.
     * @return Random number selected. Or -1 if the remaining pool is empty.
     */
    public int next() {
      switch (n) {
      case 0: return -1;
      case 1: 
      {
        int index = mapping.get(0);
        --n;
        return index;
      }
      default:
      {
        int pos = rand.nextInt(n);
        int index = mapping.get(pos);
        mapping.swap(pos, --n);
        return index;
      }
      }
    }

    /**
     * Get the remaining random number pool size.
     */
    public int getPoolSize() {
      return n;
    }
    
    /**
     * Reset the selector for reuse usage.
     */
    public void reset() {
      mapping.reset();
      n = mapping.getSize();
    }
  }
  
  
  /**
   * Selecting m random integers from 0..n-1.
   * @return An array of selected integers.
   */
  public static int[] select(int m, int n, Random rand) {
    if (m >= n) {
      int[] ret = new int[n];
      for (int i=0; i<n; ++i) {
        ret[i] = i;
      }
      return ret;
    }
    
    Selector selector = new Selector(n, (float)m/n, rand);
    int[] selected = new int[m];
    for (int i=0; i<m; ++i) {
      selected[i] = selector.next();
    }
    return selected;
  }
}
