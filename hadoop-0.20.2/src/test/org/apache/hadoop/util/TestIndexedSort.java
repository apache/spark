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

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

public class TestIndexedSort extends TestCase {

  public void sortAllEqual(IndexedSorter sorter) throws Exception {
    final int SAMPLE = 500;
    int[] values = new int[SAMPLE];
    Arrays.fill(values, 10);
    SampleSortable s = new SampleSortable(values);
    sorter.sort(s, 0, SAMPLE);
    int[] check = s.getSorted();
    assertTrue(Arrays.toString(values) + "\ndoesn't match\n" +
        Arrays.toString(check), Arrays.equals(values, check));
    // Set random min/max, re-sort.
    Random r = new Random();
    int min = r.nextInt(SAMPLE);
    int max = (min + 1 + r.nextInt(SAMPLE - 2)) % SAMPLE;
    values[min] = 9;
    values[max] = 11;
    System.out.println("testAllEqual setting min/max at " + min + "/" + max +
        "(" + sorter.getClass().getName() + ")");
    s = new SampleSortable(values);
    sorter.sort(s, 0, SAMPLE);
    check = s.getSorted();
    Arrays.sort(values);
    assertTrue(check[0] == 9);
    assertTrue(check[SAMPLE - 1] == 11);
    assertTrue(Arrays.toString(values) + "\ndoesn't match\n" +
        Arrays.toString(check), Arrays.equals(values, check));
  }

  public void sortSorted(IndexedSorter sorter) throws Exception {
    final int SAMPLE = 500;
    int[] values = new int[SAMPLE];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("testSorted seed: " + seed +
        "(" + sorter.getClass().getName() + ")");
    for (int i = 0; i < SAMPLE; ++i) {
      values[i] = r.nextInt(100);
    }
    Arrays.sort(values);
    SampleSortable s = new SampleSortable(values);
    sorter.sort(s, 0, SAMPLE);
    int[] check = s.getSorted();
    assertTrue(Arrays.toString(values) + "\ndoesn't match\n" +
        Arrays.toString(check), Arrays.equals(values, check));
  }

  public void sortSequential(IndexedSorter sorter) throws Exception {
    final int SAMPLE = 500;
    int[] values = new int[SAMPLE];
    for (int i = 0; i < SAMPLE; ++i) {
      values[i] = i;
    }
    SampleSortable s = new SampleSortable(values);
    sorter.sort(s, 0, SAMPLE);
    int[] check = s.getSorted();
    assertTrue(Arrays.toString(values) + "\ndoesn't match\n" +
        Arrays.toString(check), Arrays.equals(values, check));
  }

  public void sortSingleRecord(IndexedSorter sorter) throws Exception {
    final int SAMPLE = 1;
    SampleSortable s = new SampleSortable(SAMPLE);
    int[] values = s.getValues();
    sorter.sort(s, 0, SAMPLE);
    int[] check = s.getSorted();
    assertTrue(Arrays.toString(values) + "\ndoesn't match\n" +
        Arrays.toString(check), Arrays.equals(values, check));
  }

  public void sortRandom(IndexedSorter sorter) throws Exception {
    final int SAMPLE = 256 * 1024;
    SampleSortable s = new SampleSortable(SAMPLE);
    long seed = s.getSeed();
    System.out.println("sortRandom seed: " + seed +
        "(" + sorter.getClass().getName() + ")");
    int[] values = s.getValues();
    Arrays.sort(values);
    sorter.sort(s, 0, SAMPLE);
    int[] check = s.getSorted();
    assertTrue("seed: " + seed + "\ndoesn't match\n",
               Arrays.equals(values, check));
  }

  public void sortWritable(IndexedSorter sorter) throws Exception {
    final int SAMPLE = 1000;
    WritableSortable s = new WritableSortable(SAMPLE);
    long seed = s.getSeed();
    System.out.println("sortWritable seed: " + seed +
        "(" + sorter.getClass().getName() + ")");
    String[] values = s.getValues();
    Arrays.sort(values);
    sorter.sort(s, 0, SAMPLE);
    String[] check = s.getSorted();
    assertTrue("seed: " + seed + "\ndoesn't match",
               Arrays.equals(values, check));
  }


  public void testQuickSort() throws Exception {
    QuickSort sorter = new QuickSort();
    sortRandom(sorter);
    sortSingleRecord(sorter);
    sortSequential(sorter);
    sortSorted(sorter);
    sortAllEqual(sorter);
    sortWritable(sorter);

    // test degenerate case for median-of-three partitioning
    // a_n, a_1, a_2, ..., a_{n-1}
    final int DSAMPLE = 500;
    int[] values = new int[DSAMPLE];
    for (int i = 0; i < DSAMPLE; ++i) { values[i] = i; }
    values[0] = values[DSAMPLE - 1] + 1;
    SampleSortable s = new SampleSortable(values);
    values = s.getValues();
    final int DSS = (DSAMPLE / 2) * (DSAMPLE / 2);
    // Worst case is (N/2)^2 comparisons, not including those effecting
    // the median-of-three partitioning; impl should handle this case
    MeasuredSortable m = new MeasuredSortable(s, DSS);
    sorter.sort(m, 0, DSAMPLE);
    System.out.println("QuickSort degen cmp/swp: " +
        m.getCmp() + "/" + m.getSwp() +
        "(" + sorter.getClass().getName() + ")");
    Arrays.sort(values);
    int[] check = s.getSorted();
    assertTrue(Arrays.equals(values, check));
  }

  public void testHeapSort() throws Exception {
    HeapSort sorter = new HeapSort();
    sortRandom(sorter);
    sortSingleRecord(sorter);
    sortSequential(sorter);
    sortSorted(sorter);
    sortAllEqual(sorter);
    sortWritable(sorter);
  }

  // Sortables //

  private static class SampleSortable implements IndexedSortable {
    private int[] valindex;
    private int[] valindirect;
    private int[] values;
    private final long seed;

    public SampleSortable() {
      this(50);
    }

    public SampleSortable(int j) {
      Random r = new Random();
      seed = r.nextLong();
      r.setSeed(seed);
      values = new int[j];
      valindex = new int[j];
      valindirect = new int[j];
      for (int i = 0; i < j; ++i) {
        valindex[i] = valindirect[i] = i;
        values[i] = r.nextInt(1000);
      }
    }

    public SampleSortable(int[] values) {
      this.values = values;
      valindex = new int[values.length];
      valindirect = new int[values.length];
      for (int i = 0; i < values.length; ++i) {
        valindex[i] = valindirect[i] = i;
      }
      seed = 0;
    }

    public long getSeed() {
      return seed;
    }

    public int compare(int i, int j) {
      // assume positive
      return
        values[valindirect[valindex[i]]] - values[valindirect[valindex[j]]];
    }

    public void swap(int i, int j) {
      int tmp = valindex[i];
      valindex[i] = valindex[j];
      valindex[j] = tmp;
    }

    public int[] getSorted() {
      int[] ret = new int[values.length];
      for (int i = 0; i < ret.length; ++i) {
        ret[i] = values[valindirect[valindex[i]]];
      }
      return ret;
    }

    public int[] getValues() {
      int[] ret = new int[values.length];
      System.arraycopy(values, 0, ret, 0, values.length);
      return ret;
    }

  }

  public static class MeasuredSortable implements IndexedSortable {

    private int comparisions;
    private int swaps;
    private final int maxcmp;
    private final int maxswp;
    private IndexedSortable s;

    public MeasuredSortable(IndexedSortable s) {
      this(s, Integer.MAX_VALUE);
    }

    public MeasuredSortable(IndexedSortable s, int maxcmp) {
      this(s, maxcmp, Integer.MAX_VALUE);
    }

    public MeasuredSortable(IndexedSortable s, int maxcmp, int maxswp) {
      this.s = s;
      this.maxcmp = maxcmp;
      this.maxswp = maxswp;
    }

    public int getCmp() { return comparisions; }
    public int getSwp() { return swaps; }

    public int compare(int i, int j) {
      assertTrue("Expected fewer than " + maxcmp + " comparisons",
                 ++comparisions < maxcmp);
      return s.compare(i, j);
    }

    public void swap(int i, int j) {
      assertTrue("Expected fewer than " + maxswp + " swaps",
                 ++swaps < maxswp);
      s.swap(i, j);
    }

  }

  private static class WritableSortable implements IndexedSortable {

    private static Random r = new Random();
    private final int eob;
    private final int[] indices;
    private final int[] offsets;
    private final byte[] bytes;
    private final WritableComparator comparator;
    private final String[] check;
    private final long seed;

    public WritableSortable() throws IOException {
      this(100);
    }

    public WritableSortable(int j) throws IOException {
      seed = r.nextLong();
      r.setSeed(seed);
      Text t = new Text();
      StringBuffer sb = new StringBuffer();
      indices = new int[j];
      offsets = new int[j];
      check = new String[j];
      DataOutputBuffer dob = new DataOutputBuffer();
      for (int i = 0; i < j; ++i) {
        indices[i] = i;
        offsets[i] = dob.getLength();
        genRandom(t, r.nextInt(15) + 1, sb);
        t.write(dob);
        check[i] = t.toString();
      }
      eob = dob.getLength();
      bytes = dob.getData();
      comparator = WritableComparator.get(Text.class);
    }

    public long getSeed() {
      return seed;
    }

    private static void genRandom(Text t, int len, StringBuffer sb) {
      sb.setLength(0);
      for (int i = 0; i < len; ++i) {
        sb.append(Integer.toString(r.nextInt(26) + 10, 36));
      }
      t.set(sb.toString());
    }

    public int compare(int i, int j) {
      final int ii = indices[i];
      final int ij = indices[j];
      return comparator.compare(bytes, offsets[ii],
        ((ii + 1 == indices.length) ? eob : offsets[ii + 1]) - offsets[ii],
        bytes, offsets[ij],
        ((ij + 1 == indices.length) ? eob : offsets[ij + 1]) - offsets[ij]);
    }

    public void swap(int i, int j) {
      int tmp = indices[i];
      indices[i] = indices[j];
      indices[j] = tmp;
    }

    public String[] getValues() {
      return check;
    }

    public String[] getSorted() throws IOException {
      String[] ret = new String[indices.length];
      Text t = new Text();
      DataInputBuffer dib = new DataInputBuffer();
      for (int i = 0; i < ret.length; ++i) {
        int ii = indices[i];
        dib.reset(bytes, offsets[ii],
        ((ii + 1 == indices.length) ? eob : offsets[ii + 1]) - offsets[ii]);
        t.readFields(dib);
        ret[i] = t.toString();
      }
      return ret;
    }

  }

}
