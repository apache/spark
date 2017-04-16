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

package org.apache.hadoop.mapred;

import junit.framework.TestCase;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.Iterator;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

public class TestMapCollection extends TestCase {

  private static final Log LOG = LogFactory.getLog(
      TestMapCollection.class.getName());

  public static class KeyWritable
      implements WritableComparable<KeyWritable>, JobConfigurable {

    private final byte c = (byte)('K' & 0xFF);
    static private boolean pedantic = false;
    protected int expectedlen;

    public void configure(JobConf conf) {
      expectedlen = conf.getInt("test.keywritable.length", 1);
      pedantic = conf.getBoolean("test.pedantic.verification", false);
    }

    public KeyWritable() { }

    public KeyWritable(int len) {
      this();
      expectedlen = len;
    }

    public int getLength() {
      return expectedlen;
    }

    public int compareTo(KeyWritable o) {
      if (o == this) return 0;
      return expectedlen - o.getLength();
    }

    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof KeyWritable)) return false;
      return 0 == compareTo((KeyWritable)o);
    }

    public int hashCode() {
      return 37 * expectedlen;
    }

    public void readFields(DataInput in) throws IOException {
      if (expectedlen != 0) {
        int bytesread;
        if (pedantic) {
          for (int i = 0; i < expectedlen; ++i)
            assertEquals("Invalid byte at " + i, c, in.readByte());
          bytesread = expectedlen;
        } else {
          bytesread = in.skipBytes(expectedlen);
        }
        assertEquals("Too few bytes in record", expectedlen, bytesread);
      }
      // cannot verify that the stream has been exhausted
    }

    public void write(DataOutput out) throws IOException {
      if (expectedlen != 0) {
        if (expectedlen > 1024) {
          byte[] b = new byte[expectedlen];
          Arrays.fill(b, c);
          out.write(b);
        } else {
          for (int i = 0; i < expectedlen; ++i) {
            out.write(c);
          }
        }
      }
    }

    public static class Comparator extends WritableComparator {
      public Comparator() {
        super(KeyWritable.class);
      }

      public int compare(byte[] b1, int s1, int l1,
                         byte[] b2, int s2, int l2) {
        if (pedantic) {
          for (int i = s1; i < l1; ++i) {
            assertEquals("Invalid key at " + s1, b1[i], (byte)('K' & 0xFF));
          }
          for (int i = s2; i < l2; ++i) {
            assertEquals("Invalid key at " + s2, b2[i], (byte)('K' & 0xFF));
          }
        }
        return l1 - l2;
      }
    }


    static {
      WritableComparator.define(KeyWritable.class, new Comparator());
    }
  }

  public static class ValWritable extends KeyWritable {

    private final byte c = (byte)('V' & 0xFF);

    public ValWritable() { }

    public ValWritable(int len) {
      this();
      expectedlen = len;
    }

    public void configure(JobConf conf) {
      expectedlen = conf.getInt("test.valwritable.length", 1);
    }
  }

  public static class SpillMapper
      implements Mapper<NullWritable,NullWritable,KeyWritable,ValWritable> {

    private int keylen = 1;
    private int vallen = 1;
    private int numrecs = 100;

    public void configure(JobConf job) {
      keylen = job.getInt("test.keywritable.length", 1);
      vallen = job.getInt("test.valwritable.length", 1);
      numrecs = job.getInt("test.spillmap.records", 100);
    }

    public void map(NullWritable key, NullWritable value,
        OutputCollector<KeyWritable,ValWritable> out, Reporter reporter)
        throws IOException {
      KeyWritable k = new KeyWritable(keylen);
      ValWritable v = new ValWritable(vallen);
      for (int i = 0; i < numrecs; ++i) {
        if ((i % 1000) == 0) {
          reporter.progress();
        }
        out.collect(k, v);
      }
    }

    public void close() { }

  }

  public static class SpillReducer
      implements Reducer<KeyWritable,ValWritable,NullWritable,NullWritable> {

    private int numrecs = 100;

    public void configure(JobConf job) {
      numrecs = job.getInt("test.spillmap.records", 100);
    }

    public void reduce(KeyWritable k, Iterator<ValWritable> values,
        OutputCollector<NullWritable,NullWritable> out, Reporter reporter) {
      int i = 0;
      while (values.hasNext()) {
        values.next();
        ++i;
      }
      assertEquals("Unexpected record count (" + i + "/" +
                   numrecs + ")", numrecs, i);
    }

    public void close() { }

  }

  public static class FakeSplit implements InputSplit {
    public void write(DataOutput out) throws IOException { }
    public void readFields(DataInput in) throws IOException { }
    public long getLength() { return 0L; }
    public String[] getLocations() { return new String[0]; }
  }

  public static class FakeIF
      implements InputFormat<NullWritable,NullWritable> {

    public FakeIF() { }

    public InputSplit[] getSplits(JobConf conf, int numSplits) {
      InputSplit[] splits = new InputSplit[numSplits];
      for (int i = 0; i < splits.length; ++i) {
        splits[i] = new FakeSplit();
      }
      return splits;
    }

    public RecordReader<NullWritable,NullWritable> getRecordReader(
        InputSplit ignored, JobConf conf, Reporter reporter) {
      return new RecordReader<NullWritable,NullWritable>() {
        private boolean done = false;
        public boolean next(NullWritable key, NullWritable value)
            throws IOException {
          if (done)
            return false;
          done = true;
          return true;
        }
        public NullWritable createKey() { return NullWritable.get(); }
        public NullWritable createValue() { return NullWritable.get(); }
        public long getPos() throws IOException { return 0L; }
        public void close() throws IOException { }
        public float getProgress() throws IOException { return 0.0f; }
      };
    }
  }

  private static void runTest(String name, int keylen, int vallen,
      int records, int ioSortMB, float recPer, float spillPer,
      boolean pedantic) throws Exception {
    JobConf conf = new JobConf(new Configuration(), SpillMapper.class);

    conf.setInt("io.sort.mb", ioSortMB);
    conf.set("io.sort.record.percent", Float.toString(recPer));
    conf.set("io.sort.spill.percent", Float.toString(spillPer));

    conf.setInt("test.keywritable.length", keylen);
    conf.setInt("test.valwritable.length", vallen);
    conf.setInt("test.spillmap.records", records);
    conf.setBoolean("test.pedantic.verification", pedantic);

    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    conf.setInputFormat(FakeIF.class);
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setMapperClass(SpillMapper.class);
    conf.setReducerClass(SpillReducer.class);
    conf.setMapOutputKeyClass(KeyWritable.class);
    conf.setMapOutputValueClass(ValWritable.class);

    LOG.info("Running " + name);
    JobClient.runJob(conf);
  }

  private static void runTest(String name, int keylen, int vallen, int records,
      boolean pedantic) throws Exception {
    runTest(name, keylen, vallen, records, 1, 0.05f, .8f, pedantic);
  }

  public void testLastFill() throws Exception {
    // last byte of record/key is the last/first byte in the spill buffer
    runTest("vallastbyte", 128, 896, 1344, 1, 0.125f, 0.5f, true);
    runTest("keylastbyte", 512, 1024, 896, 1, 0.125f, 0.5f, true);
  }

  public void testLargeRecords() throws Exception {
    // maps emitting records larger than io.sort.mb
    runTest("largerec", 100, 1024*1024, 5, false);
    runTest("largekeyzeroval", 1024*1024, 0, 5, false);
  }

  public void testSpillPer() throws Exception {
    // set non-default, 100% speculative spill boundary
    runTest("fullspill2B", 1, 1, 10000, 1, 0.05f, 1.0f, true);
    runTest("fullspill200B", 100, 100, 10000, 1, 0.05f, 1.0f, true);
    runTest("fullspillbuf", 10 * 1024, 20 * 1024, 256, 1, 0.3f, 1.0f, true);
    runTest("lt50perspill", 100, 100, 10000, 1, 0.05f, 0.3f, true);
  }

  public void testZeroLength() throws Exception {
    // test key/value at zero-length
    runTest("zeroval", 1, 0, 10000, true);
    runTest("zerokey", 0, 1, 10000, true);
    runTest("zerokeyval", 0, 0, 10000, false);
    runTest("zerokeyvalfull", 0, 0, 10000, 1, 0.05f, 1.0f, false);
  }

}
