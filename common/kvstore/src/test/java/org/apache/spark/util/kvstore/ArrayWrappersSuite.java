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

package org.apache.spark.util.kvstore;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ArrayWrappersSuite {

  @Test
  public void testGenericArrayKey() {
   byte[] b1 = new byte[] { 0x01, 0x02, 0x03 };
   byte[] b2 = new byte[] { 0x01, 0x02 };
   int[] i1 = new int[] { 1, 2, 3 };
   int[] i2 = new int[] { 1, 2 };
   String[] s1 = new String[] { "1", "2", "3" };
   String[] s2 = new String[] { "1", "2" };

   assertEquals(ArrayWrappers.forArray(b1), ArrayWrappers.forArray(b1));
   assertNotEquals(ArrayWrappers.forArray(b1), ArrayWrappers.forArray(b2));
   assertNotEquals(ArrayWrappers.forArray(b1), ArrayWrappers.forArray(i1));
   assertNotEquals(ArrayWrappers.forArray(b1), ArrayWrappers.forArray(s1));

   assertEquals(ArrayWrappers.forArray(i1), ArrayWrappers.forArray(i1));
   assertNotEquals(ArrayWrappers.forArray(i1), ArrayWrappers.forArray(i2));
   assertNotEquals(ArrayWrappers.forArray(i1), ArrayWrappers.forArray(b1));
   assertNotEquals(ArrayWrappers.forArray(i1), ArrayWrappers.forArray(s1));

   assertEquals(ArrayWrappers.forArray(s1), ArrayWrappers.forArray(s1));
   assertNotEquals(ArrayWrappers.forArray(s1), ArrayWrappers.forArray(s2));
   assertNotEquals(ArrayWrappers.forArray(s1), ArrayWrappers.forArray(b1));
   assertNotEquals(ArrayWrappers.forArray(s1), ArrayWrappers.forArray(i1));

   assertEquals(0, ArrayWrappers.forArray(b1).compareTo(ArrayWrappers.forArray(b1)));
   assertTrue(ArrayWrappers.forArray(b1).compareTo(ArrayWrappers.forArray(b2)) > 0);

   assertEquals(0, ArrayWrappers.forArray(i1).compareTo(ArrayWrappers.forArray(i1)));
   assertTrue(ArrayWrappers.forArray(i1).compareTo(ArrayWrappers.forArray(i2)) > 0);

   assertEquals(0, ArrayWrappers.forArray(s1).compareTo(ArrayWrappers.forArray(s1)));
   assertTrue(ArrayWrappers.forArray(s1).compareTo(ArrayWrappers.forArray(s2)) > 0);
  }

  @Test
  public void testOppositeSignArrayKeyComparison() {
    int[] minInt = new int[] { Integer.MIN_VALUE };
    int[] maxInt = new int[] { Integer.MAX_VALUE };
    assertTrue(ArrayWrappers.forArray(minInt).compareTo(ArrayWrappers.forArray(maxInt)) < 0);
    assertTrue(ArrayWrappers.forArray(maxInt).compareTo(ArrayWrappers.forArray(minInt)) > 0);

    long[] minLong = new long[] { Long.MIN_VALUE };
    long[] maxLong = new long[] { Long.MAX_VALUE };
    assertTrue(ArrayWrappers.forArray(minLong).compareTo(ArrayWrappers.forArray(maxLong)) < 0);
    assertTrue(ArrayWrappers.forArray(maxLong).compareTo(ArrayWrappers.forArray(minLong)) > 0);
  }

}
