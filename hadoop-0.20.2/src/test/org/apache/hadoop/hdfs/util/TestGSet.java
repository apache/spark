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
package org.apache.hadoop.hdfs.util;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class TestGSet {
  private static final Random ran = new Random();
  private static final long starttime = System.currentTimeMillis();

  private static void print(Object s) {
    System.out.print(s);
    System.out.flush();
  }

  private static void println(Object s) {
    System.out.println(s);
  }

  @Test
  public void testExceptionCases() {
    {
      //test contains
      final LightWeightGSet<Integer, Integer> gset
        = new LightWeightGSet<Integer, Integer>(16);
      try {
        //test contains with a null element
        gset.contains(null);
        Assert.fail();
      } catch(NullPointerException e) {
        LightWeightGSet.LOG.info("GOOD: getting " + e, e);
      }
    }

    {
      //test get
      final LightWeightGSet<Integer, Integer> gset
        = new LightWeightGSet<Integer, Integer>(16);
      try {
        //test get with a null element
        gset.get(null);
        Assert.fail();
      } catch(NullPointerException e) {
        LightWeightGSet.LOG.info("GOOD: getting " + e, e);
      }
    }

    {
      //test put
      final LightWeightGSet<Integer, Integer> gset
        = new LightWeightGSet<Integer, Integer>(16);
      try {
        //test put with a null element
        gset.put(null);
        Assert.fail();
      } catch(NullPointerException e) {
        LightWeightGSet.LOG.info("GOOD: getting " + e, e);
      }
      try {
        //test putting an element which is not implementing LinkedElement
        gset.put(1);
        Assert.fail();
      } catch(IllegalArgumentException e) {
        LightWeightGSet.LOG.info("GOOD: getting " + e, e);
      }
    }

    {
      //test iterator
      final IntElement[] data = new IntElement[5];
      for(int i = 0; i < data.length; i++) {
        data[i] = new IntElement(i, i);
      }

      for(int v = 1; v < data.length-1; v++) {
        {
          //test remove while iterating
          final GSet<IntElement, IntElement> gset = createGSet(data);
          for(IntElement i : gset) {
            if (i.value == v) {
              //okay because data[0] is not in gset
              gset.remove(data[0]);
            }
          }

          try {
            //exception because data[1] is in gset
            for(IntElement i : gset) {
              if (i.value == v) {
                gset.remove(data[1]);
              }
            }
            Assert.fail();
          } catch(ConcurrentModificationException e) {
            LightWeightGSet.LOG.info("GOOD: getting " + e, e);
          }
        }

        {
          //test put new element while iterating
          final GSet<IntElement, IntElement> gset = createGSet(data);
          try {
            for(IntElement i : gset) {
              if (i.value == v) {
                gset.put(data[0]);
              }
            }
            Assert.fail();
          } catch(ConcurrentModificationException e) {
            LightWeightGSet.LOG.info("GOOD: getting " + e, e);
          }
        }

        {
          //test put existing element while iterating
          final GSet<IntElement, IntElement> gset = createGSet(data);
          try {
            for(IntElement i : gset) {
              if (i.value == v) {
                gset.put(data[3]);
              }
            }
            Assert.fail();
          } catch(ConcurrentModificationException e) {
            LightWeightGSet.LOG.info("GOOD: getting " + e, e);
          }
        }
      }
    }
  }

  private static GSet<IntElement, IntElement> createGSet(final IntElement[] data) {
    final GSet<IntElement, IntElement> gset
      = new LightWeightGSet<IntElement, IntElement>(8);
    for(int i = 1; i < data.length; i++) {
      gset.put(data[i]);
    }
    return gset;
  }

  @Test
  public void testGSet() {
    //The parameters are: table length, data size, modulus.
    check(new GSetTestCase(1, 1 << 4, 65537));
    check(new GSetTestCase(17, 1 << 16, 17));
    check(new GSetTestCase(255, 1 << 10, 65537));
  }

  /**
   * A long test,
   * which may take ~5 hours,
   * with various data sets and parameters.
   * If you are changing the implementation,
   * please un-comment the following line in order to run the test.
   */
  //@Test
  public void runMultipleTestGSet() {
    for(int offset = -2; offset <= 2; offset++) {
      runTestGSet(1, offset);
      for(int i = 1; i < Integer.SIZE - 1; i++) {
        runTestGSet((1 << i) + 1, offset);
      }
    }
  }

  private static void runTestGSet(final int modulus, final int offset) {
    println("\n\nmodulus=" + modulus + ", offset=" + offset);
    for(int i = 0; i <= 16; i += 4) {
      final int tablelength = (1 << i) + offset;

      final int upper = i + 2;
      final int steps = Math.max(1, upper/3);

      for(int j = 0; j <= upper; j += steps) {
        final int datasize = 1 << j;
        check(new GSetTestCase(tablelength, datasize, modulus));
      }
    }
  }

  private static void check(final GSetTestCase test) {
    //check add
    print("  check add .................. ");
    for(int i = 0; i < test.data.size()/2; i++) {
      test.put(test.data.get(i));
    }
    for(int i = 0; i < test.data.size(); i++) {
      test.put(test.data.get(i));
    }
    println("DONE " + test.stat());

    //check remove and add
    print("  check remove & add ......... ");
    for(int j = 0; j < 10; j++) {
      for(int i = 0; i < test.data.size()/2; i++) {
        final int r = ran.nextInt(test.data.size());
        test.remove(test.data.get(r));
      }
      for(int i = 0; i < test.data.size()/2; i++) {
        final int r = ran.nextInt(test.data.size());
        test.put(test.data.get(r));
      }
    }
    println("DONE " + test.stat());

    //check remove
    print("  check remove ............... ");
    for(int i = 0; i < test.data.size(); i++) {
      test.remove(test.data.get(i));
    }
    Assert.assertEquals(0, test.gset.size());
    println("DONE " + test.stat());

    //check remove and add again
    print("  check remove & add again ... ");
    for(int j = 0; j < 10; j++) {
      for(int i = 0; i < test.data.size()/2; i++) {
        final int r = ran.nextInt(test.data.size());
        test.remove(test.data.get(r));
      }
      for(int i = 0; i < test.data.size()/2; i++) {
        final int r = ran.nextInt(test.data.size());
        test.put(test.data.get(r));
      }
    }
    println("DONE " + test.stat());

    final long s = (System.currentTimeMillis() - starttime)/1000L;
    println("total time elapsed=" + s + "s\n");
  }

  /** Test cases */
  private static class GSetTestCase implements GSet<IntElement, IntElement> {
    final GSet<IntElement, IntElement> expected
        = new GSetByHashMap<IntElement, IntElement>(1024, 0.75f);
    final GSet<IntElement, IntElement> gset;
    final IntData data;

    final String info;
    final long starttime = System.currentTimeMillis();
    /** Determine the probability in {@link #check()}. */
    final int denominator;
    int iterate_count = 0;
    int contain_count = 0;

    GSetTestCase(int tablelength, int datasize, int modulus) {
      denominator = Math.min((datasize >> 7) + 1, 1 << 16);
      info = getClass().getSimpleName()
          + ": tablelength=" + tablelength
          + ", datasize=" + datasize
          + ", modulus=" + modulus
          + ", denominator=" + denominator;
      println(info);

      data  = new IntData(datasize, modulus);
      gset = new LightWeightGSet<IntElement, IntElement>(tablelength);

      Assert.assertEquals(0, gset.size());
    }

    private boolean containsTest(IntElement key) {
      final boolean e = expected.contains(key);
      Assert.assertEquals(e, gset.contains(key));
      return e;
    }
    @Override
    public boolean contains(IntElement key) {
      final boolean e = containsTest(key);
      check();
      return e;
    }

    private IntElement getTest(IntElement key) {
      final IntElement e = expected.get(key);
      Assert.assertEquals(e.id, gset.get(key).id);
      return e;
    }
    @Override
    public IntElement get(IntElement key) {
      final IntElement e = getTest(key);
      check();
      return e;
    }

    private IntElement putTest(IntElement element) {
      final IntElement e = expected.put(element);
      if (e == null) {
        Assert.assertEquals(null, gset.put(element));
      } else {
        Assert.assertEquals(e.id, gset.put(element).id);
      }
      return e;
    }
    @Override
    public IntElement put(IntElement element) {
      final IntElement e = putTest(element);
      check();
      return e;
    }

    private IntElement removeTest(IntElement key) {
      final IntElement e = expected.remove(key);
      if (e == null) {
        Assert.assertEquals(null, gset.remove(key));
      } else {
        Assert.assertEquals(e.id, gset.remove(key).id);
      }

      check();
      return e;
    }
    @Override
    public IntElement remove(IntElement key) {
      final IntElement e = removeTest(key);
      check();
      return e;
    }

    private int sizeTest() {
      final int s = expected.size();
      Assert.assertEquals(s, gset.size());
      return s;
    }
    @Override
    public int size() {
      final int s = sizeTest();
      check();
      return s;
    }

    @Override
    public Iterator<IntElement> iterator() {
      throw new UnsupportedOperationException();
    }

    void check() {
      //test size
      sizeTest();

      if (ran.nextInt(denominator) == 0) {
        //test get(..), check content and test iterator
        iterate_count++;
        for(IntElement i : gset) {
          getTest(i);
        }
      }

      if (ran.nextInt(denominator) == 0) {
        //test contains(..)
        contain_count++;
        final int count = Math.min(data.size(), 1000);
        if (count == data.size()) {
          for(IntElement i : data.integers) {
            containsTest(i);
          }
        } else {
          for(int j = 0; j < count; j++) {
            containsTest(data.get(ran.nextInt(data.size())));
          }
        }
      }
    }

    String stat() {
      final long t = System.currentTimeMillis() - starttime;
      return String.format(" iterate=%5d, contain=%5d, time elapsed=%5d.%03ds",
          iterate_count, contain_count, t/1000, t%1000);
    }
  }

  /** Test data set */
  private static class IntData {
    final IntElement[] integers;

    IntData(int size, int modulus) {
      integers = new IntElement[size];
      for(int i = 0; i < integers.length; i++) {
        integers[i] = new IntElement(i, ran.nextInt(modulus));
      }
    }

    IntElement get(int i) {
      return integers[i];
    }

    int size() {
      return integers.length;
    }
  }

  /** Elements of {@link LightWeightGSet} in this test */
  private static class IntElement implements LightWeightGSet.LinkedElement,
      Comparable<IntElement> {
    private LightWeightGSet.LinkedElement next;
    final int id;
    final int value;

    IntElement(int id, int value) {
      this.id = id;
      this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
      return obj != null && obj instanceof IntElement
          && value == ((IntElement)obj).value;
    }

    @Override
    public int hashCode() {
      return value;
    }

    @Override
    public int compareTo(IntElement that) {
      return value - that.value;
    }

    @Override
    public String toString() {
      return id + "#" + value;
    }

    @Override
    public LightWeightGSet.LinkedElement getNext() {
      return next;
    }

    @Override
    public void setNext(LightWeightGSet.LinkedElement e) {
      next = e;
    }
  }
}
