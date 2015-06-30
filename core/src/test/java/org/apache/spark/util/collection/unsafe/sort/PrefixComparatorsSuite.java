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

import org.junit.Test;
import static org.junit.Assert.*;

public class PrefixComparatorsSuite {

  private static int genericComparison(Comparable a, Comparable b) {
    return a.compareTo(b);
  }

  @Test
  public void intPrefixComparator() {
    int[] testData = new int[] { 0, Integer.MIN_VALUE, Integer.MAX_VALUE, 0, 1, 2, -1, -2, 1024};
    for (int a : testData) {
      for (int b : testData) {
        long aPrefix = PrefixComparators.INTEGER.computePrefix(a);
        long bPrefix = PrefixComparators.INTEGER.computePrefix(b);
        assertEquals(
          "Wrong prefix comparison results for a=" + a + " b=" + b,
          genericComparison(a, b),
          PrefixComparators.INTEGER.compare(aPrefix, bPrefix));

      }
    }
  }

}
