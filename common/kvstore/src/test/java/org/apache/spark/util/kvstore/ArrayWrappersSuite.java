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
  public void testIntArrayCompareNoOverflow() {
    // Large opposite-sign values overflow a subtraction-based comparison, so these
    // assertions fail unless compareTo uses Integer.compare.
    Comparable<Object> min = ArrayWrappers.forArray(new int[] { Integer.MIN_VALUE });
    Comparable<Object> max = ArrayWrappers.forArray(new int[] { Integer.MAX_VALUE });
    Comparable<Object> negOne = ArrayWrappers.forArray(new int[] { -1 });

    assertTrue(min.compareTo(max) < 0);
    assertTrue(max.compareTo(min) > 0);
    assertTrue(max.compareTo(negOne) > 0);
    assertTrue(negOne.compareTo(max) < 0);

    // Ordering must stay correct beyond the first element as well.
    Comparable<Object> a = ArrayWrappers.forArray(new int[] { 0, Integer.MIN_VALUE });
    Comparable<Object> b = ArrayWrappers.forArray(new int[] { 0, Integer.MAX_VALUE });
    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
  }

  @Test
  public void testLongArrayCompareNoOverflow() {
    // Large opposite-sign values overflow a subtraction-based comparison, so these
    // assertions fail unless compareTo uses Long.compare.
    Comparable<Object> min = ArrayWrappers.forArray(new long[] { Long.MIN_VALUE });
    Comparable<Object> max = ArrayWrappers.forArray(new long[] { Long.MAX_VALUE });
    Comparable<Object> negOne = ArrayWrappers.forArray(new long[] { -1L });

    assertTrue(min.compareTo(max) < 0);
    assertTrue(max.compareTo(min) > 0);
    assertTrue(max.compareTo(negOne) > 0);
    assertTrue(negOne.compareTo(max) < 0);

    // Ordering must stay correct beyond the first element as well.
    Comparable<Object> a = ArrayWrappers.forArray(new long[] { 0L, Long.MIN_VALUE });
    Comparable<Object> b = ArrayWrappers.forArray(new long[] { 0L, Long.MAX_VALUE });
    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
  }

}
