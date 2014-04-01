/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import com.sun.tools.javac.code.Attribute.Array;

public class TestRandomAlgorithm {
  private static final int[][] parameters = new int[][] {
    {5, 1, 1}, 
    {10, 1, 2},
    {10, 2, 2},
    {20, 1, 3},
    {20, 2, 3},
    {20, 3, 3},
    {100, 3, 10},
    {100, 3, 100},
    {100, 3, 1000},
    {100, 3, 10000},
    {100, 3, 100000},
    {100, 3, 1000000}
  };
  
  private List<Integer> convertIntArray(int[] from) {
    List<Integer> ret = new ArrayList<Integer>(from.length);
    for (int v : from) {
      ret.add(v);
    }
    return ret;
  }
  
  private void testRandomSelectSelector(int niter, int m, int n) {
    RandomAlgorithms.Selector selector = new RandomAlgorithms.Selector(n,
        (double) m / n, new Random());
    Map<List<Integer>, Integer> results = new HashMap<List<Integer>, Integer>(
        niter);
    for (int i = 0; i < niter; ++i, selector.reset()) {
      int[] result = new int[m];
      for (int j = 0; j < m; ++j) {
        int v = selector.next();
        if (v < 0)
          break;
        result[j]=v;
      }
      Arrays.sort(result);
      List<Integer> resultAsList = convertIntArray(result);
      Integer count = results.get(resultAsList);
      if (count == null) {
        results.put(resultAsList, 1);
      } else {
        results.put(resultAsList, ++count);
      }
    }

    verifyResults(results, m, n);
  }

  private void testRandomSelect(int niter, int m, int n) {
    Random random = new Random();
    Map<List<Integer>, Integer> results = new HashMap<List<Integer>, Integer>(
        niter);
    for (int i = 0; i < niter; ++i) {
      int[] result = RandomAlgorithms.select(m, n, random);
      Arrays.sort(result);
      List<Integer> resultAsList = convertIntArray(result);
      Integer count = results.get(resultAsList);
      if (count == null) {
        results.put(resultAsList, 1);
      } else {
        results.put(resultAsList, ++count);
      }
    }

    verifyResults(results, m, n);
  }

  private void verifyResults(Map<List<Integer>, Integer> results, int m, int n) {
    if (n>=10) {
      assertTrue(results.size() >= Math.min(m, 2));
    }
    for (List<Integer> result : results.keySet()) {
      assertEquals(m, result.size());
      Set<Integer> seen = new HashSet<Integer>();
      for (int v : result) {
        System.out.printf("%d ", v);
        assertTrue((v >= 0) && (v < n));
        assertTrue(seen.add(v));
      }
      System.out.printf(" ==> %d\n", results.get(result));
    }
    System.out.println("====");
  }
  
  @Test
  public void testRandomSelect() {
    for (int[] param : parameters) {
    testRandomSelect(param[0], param[1], param[2]);
    }
  }
  
  @Test
  public void testRandomSelectSelector() {
    for (int[] param : parameters) {
      testRandomSelectSelector(param[0], param[1], param[2]);
      }
  }
}
