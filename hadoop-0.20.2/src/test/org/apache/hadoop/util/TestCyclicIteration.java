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
package org.apache.hadoop.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TestCyclicIteration extends junit.framework.TestCase {
  public void testCyclicIteration() throws Exception {
    for(int n = 0; n < 5; n++) {
      checkCyclicIteration(n);
    }
  }

  private static void checkCyclicIteration(int numOfElements) {
    //create a tree map
    final NavigableMap<Integer, Integer> map = new TreeMap<Integer, Integer>();
    final Integer[] integers = new Integer[numOfElements];
    for(int i = 0; i < integers.length; i++) {
      integers[i] = 2*i;
      map.put(integers[i], integers[i]);
    }
    System.out.println("\n\nintegers=" + Arrays.asList(integers));
    System.out.println("map=" + map);

    //try starting everywhere
    for(int start = -1; start <= 2*integers.length - 1; start++) {
      //get a cyclic iteration
      final List<Integer> iteration = new ArrayList<Integer>(); 
      for(Map.Entry<Integer, Integer> e : new CyclicIteration<Integer, Integer>(map, start)) {
        iteration.add(e.getKey());
      }
      System.out.println("start=" + start + ", iteration=" + iteration);
      
      //verify results
      for(int i = 0; i < integers.length; i++) {
        final int j = ((start+2)/2 + i)%integers.length;
        assertEquals("i=" + i + ", j=" + j, iteration.get(i), integers[j]);
      }
    }
  }
}
