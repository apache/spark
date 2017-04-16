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

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class TestGridmixRecord {
  private static final Log LOG = LogFactory.getLog(TestGridmixRecord.class);

  static void lengthTest(GridmixRecord x, GridmixRecord y, int min,
      int max) throws Exception {
    final Random r = new Random();
    final long seed = r.nextLong();
    r.setSeed(seed);
    LOG.info("length: " + seed);
    final DataInputBuffer in = new DataInputBuffer();
    final DataOutputBuffer out1 = new DataOutputBuffer();
    final DataOutputBuffer out2 = new DataOutputBuffer();
    for (int i = min; i < max; ++i) {
      setSerialize(x, r.nextLong(), i, out1);
      // check write
      assertEquals(i, out1.getLength());
      // write to stream
      x.write(out2);
      // check read
      in.reset(out1.getData(), 0, out1.getLength());
      y.readFields(in);
      assertEquals(i, x.getSize());
      assertEquals(i, y.getSize());
    }
    // check stream read
    in.reset(out2.getData(), 0, out2.getLength());
    for (int i = min; i < max; ++i) {
      y.readFields(in);
      assertEquals(i, y.getSize());
    }
  }

  static void randomReplayTest(GridmixRecord x, GridmixRecord y, int min,
      int max) throws Exception {
    final Random r = new Random();
    final long seed = r.nextLong();
    r.setSeed(seed);
    LOG.info("randReplay: " + seed);
    final DataOutputBuffer out1 = new DataOutputBuffer();
    for (int i = min; i < max; ++i) {
      final int s = out1.getLength();
      x.setSeed(r.nextLong());
      x.setSize(i);
      x.write(out1);
      assertEquals(i, out1.getLength() - s);
    }
    final DataInputBuffer in = new DataInputBuffer();
    in.reset(out1.getData(), 0, out1.getLength());
    final DataOutputBuffer out2 = new DataOutputBuffer();
    // deserialize written records, write to separate buffer
    for (int i = min; i < max; ++i) {
      final int s = in.getPosition();
      y.readFields(in);
      assertEquals(i, in.getPosition() - s);
      y.write(out2);
    }
    // verify written contents match
    assertEquals(out1.getLength(), out2.getLength());
    // assumes that writes will grow buffer deterministically
    assertEquals("Bad test", out1.getData().length, out2.getData().length);
    assertArrayEquals(out1.getData(), out2.getData());
  }

  static void eqSeedTest(GridmixRecord x, GridmixRecord y, int max)
      throws Exception {
    final Random r = new Random();
    final long s = r.nextLong();
    r.setSeed(s);
    LOG.info("eqSeed: " + s);
    assertEquals(x.fixedBytes(), y.fixedBytes());
    final int min = x.fixedBytes() + 1;
    final DataOutputBuffer out1 = new DataOutputBuffer();
    final DataOutputBuffer out2 = new DataOutputBuffer();
    for (int i = min; i < max; ++i) {
      final long seed = r.nextLong();
      setSerialize(x, seed, i, out1);
      setSerialize(y, seed, i, out2);
      assertEquals(x, y);
      assertEquals(x.hashCode(), y.hashCode());

      // verify written contents match
      assertEquals(out1.getLength(), out2.getLength());
      // assumes that writes will grow buffer deterministically
      assertEquals("Bad test", out1.getData().length, out2.getData().length);
      assertArrayEquals(out1.getData(), out2.getData());
    }
  }

  static void binSortTest(GridmixRecord x, GridmixRecord y, int min,
      int max, WritableComparator cmp) throws Exception {
    final Random r = new Random();
    final long s = r.nextLong();
    r.setSeed(s);
    LOG.info("sort: " + s);
    final DataOutputBuffer out1 = new DataOutputBuffer();
    final DataOutputBuffer out2 = new DataOutputBuffer();
    for (int i = min; i < max; ++i) {
      final long seed1 = r.nextLong();
      setSerialize(x, seed1, i, out1);
      assertEquals(0, x.compareSeed(seed1, Math.max(0, i - x.fixedBytes())));

      final long seed2 = r.nextLong();
      setSerialize(y, seed2, i, out2);
      assertEquals(0, y.compareSeed(seed2, Math.max(0, i - x.fixedBytes())));

      // for eq sized records, ensure byte cmp where req
      final int chk = WritableComparator.compareBytes(
          out1.getData(), 0, out1.getLength(),
          out2.getData(), 0, out2.getLength());
      assertEquals(chk, x.compareTo(y));
      assertEquals(chk, cmp.compare(
            out1.getData(), 0, out1.getLength(),
            out2.getData(), 0, out2.getLength()));
      // write second copy, compare eq
      final int s1 = out1.getLength();
      x.write(out1);
      assertEquals(0, cmp.compare(out1.getData(), 0, s1,
            out1.getData(), s1, out1.getLength() - s1));
      final int s2 = out2.getLength();
      y.write(out2);
      assertEquals(0, cmp.compare(out2.getData(), 0, s2,
            out2.getData(), s2, out2.getLength() - s2));
      assertEquals(chk, cmp.compare(out1.getData(), 0, s1,
            out2.getData(), s2, out2.getLength() - s2));
    }
  }

  static void checkSpec(GridmixKey a, GridmixKey b) throws Exception {
    final Random r = new Random();
    final long s = r.nextLong();
    r.setSeed(s);
    LOG.info("spec: " + s);
    final DataInputBuffer in = new DataInputBuffer();
    final DataOutputBuffer out = new DataOutputBuffer();
    a.setType(GridmixKey.REDUCE_SPEC);
    b.setType(GridmixKey.REDUCE_SPEC);
    for (int i = 0; i < 100; ++i) {
      final int in_rec = r.nextInt(Integer.MAX_VALUE);
      a.setReduceInputRecords(in_rec);
      final int out_rec = r.nextInt(Integer.MAX_VALUE);
      a.setReduceOutputRecords(out_rec);
      final int out_bytes = r.nextInt(Integer.MAX_VALUE);
      a.setReduceOutputBytes(out_bytes);
      final int min = WritableUtils.getVIntSize(in_rec)
                    + WritableUtils.getVIntSize(out_rec)
                    + WritableUtils.getVIntSize(out_bytes);
      assertEquals(min + 2, a.fixedBytes()); // meta + vint min
      final int size = r.nextInt(1024) + a.fixedBytes() + 1;
      setSerialize(a, r.nextLong(), size, out);
      assertEquals(size, out.getLength());
      assertTrue(a.equals(a));
      assertEquals(0, a.compareTo(a));

      in.reset(out.getData(), 0, out.getLength());

      b.readFields(in);
      assertEquals(size, b.getSize());
      assertEquals(in_rec, b.getReduceInputRecords());
      assertEquals(out_rec, b.getReduceOutputRecords());
      assertEquals(out_bytes, b.getReduceOutputBytes());
      assertTrue(a.equals(b));
      assertEquals(0, a.compareTo(b));
      assertEquals(a.hashCode(), b.hashCode());
    }
  }

  static void setSerialize(GridmixRecord x, long seed, int size,
      DataOutputBuffer out) throws IOException {
    x.setSeed(seed);
    x.setSize(size);
    out.reset();
    x.write(out);
  }

  @Test
  public void testKeySpec() throws Exception {
    final int min = 5;
    final int max = 300;
    final GridmixKey a = new GridmixKey(GridmixKey.REDUCE_SPEC, 1, 0L);
    final GridmixKey b = new GridmixKey(GridmixKey.REDUCE_SPEC, 1, 0L);
    lengthTest(a, b, min, max);
    randomReplayTest(a, b, min, max);
    binSortTest(a, b, min, max, new GridmixKey.Comparator());
    // 2 fixed GR bytes, 1 type, 3 spec
    eqSeedTest(a, b, max);
    checkSpec(a, b);
  }

  @Test
  public void testKeyData() throws Exception {
    final int min = 2;
    final int max = 300;
    final GridmixKey a = new GridmixKey(GridmixKey.DATA, 1, 0L);
    final GridmixKey b = new GridmixKey(GridmixKey.DATA, 1, 0L);
    lengthTest(a, b, min, max);
    randomReplayTest(a, b, min, max);
    binSortTest(a, b, min, max, new GridmixKey.Comparator());
    // 2 fixed GR bytes, 1 type
    eqSeedTest(a, b, 300);
  }

  @Test
  public void testBaseRecord() throws Exception {
    final int min = 1;
    final int max = 300;
    final GridmixRecord a = new GridmixRecord();
    final GridmixRecord b = new GridmixRecord();
    lengthTest(a, b, min, max);
    randomReplayTest(a, b, min, max);
    binSortTest(a, b, min, max, new GridmixRecord.Comparator());
    // 2 fixed GR bytes
    eqSeedTest(a, b, 300);
  }

  public static void main(String[] argv) throws Exception {
    boolean fail = false;
    final TestGridmixRecord test = new TestGridmixRecord();
    try { test.testKeySpec(); } catch (Exception e) {
      fail = true;
      e.printStackTrace();
    }
    try {test.testKeyData(); } catch (Exception e) {
      fail = true;
      e.printStackTrace();
    }
    try {test.testBaseRecord(); } catch (Exception e) {
      fail = true;
      e.printStackTrace();
    }
    System.exit(fail ? -1 : 0);
  }

  static void printDebug(GridmixRecord a, GridmixRecord b) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    a.write(out);
    System.out.println("A " +
        Arrays.toString(Arrays.copyOf(out.getData(), out.getLength())));
    out.reset();
    b.write(out);
    System.out.println("B " +
        Arrays.toString(Arrays.copyOf(out.getData(), out.getLength())));
  }

}
