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
package org.apache.hadoop.mapred.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ReflectionUtils;

public class TestDatamerge extends TestCase {

  private static MiniDFSCluster cluster = null;
  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestDatamerge.class)) {
      protected void setUp() throws Exception {
        Configuration conf = new Configuration();
        cluster = new MiniDFSCluster(conf, 2, true, null);
      }
      protected void tearDown() throws Exception {
        if (cluster != null) {
          cluster.shutdown();
        }
      }
    };
    return setup;
  }

  private static SequenceFile.Writer[] createWriters(Path testdir,
      Configuration conf, int srcs, Path[] src) throws IOException {
    for (int i = 0; i < srcs; ++i) {
      src[i] = new Path(testdir, Integer.toString(i + 10, 36));
    }
    SequenceFile.Writer out[] = new SequenceFile.Writer[srcs];
    for (int i = 0; i < srcs; ++i) {
      out[i] = new SequenceFile.Writer(testdir.getFileSystem(conf), conf,
          src[i], IntWritable.class, IntWritable.class);
    }
    return out;
  }

  private static Path[] writeSimpleSrc(Path testdir, Configuration conf,
      int srcs) throws IOException {
    SequenceFile.Writer out[] = null;
    Path[] src = new Path[srcs];
    try {
      out = createWriters(testdir, conf, srcs, src);
      final int capacity = srcs * 2 + 1;
      IntWritable key = new IntWritable();
      IntWritable val = new IntWritable();
      for (int k = 0; k < capacity; ++k) {
        for (int i = 0; i < srcs; ++i) {
          key.set(k % srcs == 0 ? k * srcs : k * srcs + i);
          val.set(10 * k + i);
          out[i].append(key, val);
          if (i == k) {
            // add duplicate key
            out[i].append(key, val);
          }
        }
      }
    } finally {
      if (out != null) {
        for (int i = 0; i < srcs; ++i) {
          if (out[i] != null)
            out[i].close();
        }
      }
    }
    return src;
  }

  private static String stringify(IntWritable key, Writable val) {
    StringBuilder sb = new StringBuilder();
    sb.append("(" + key);
    sb.append("," + val + ")");
    return sb.toString();
  }

  private static abstract class SimpleCheckerBase<V extends Writable>
      implements Mapper<IntWritable, V, IntWritable, IntWritable>,
                 Reducer<IntWritable, IntWritable, Text, Text> {
    protected final static IntWritable one = new IntWritable(1);
    int srcs;
    public void close() { }
    public void configure(JobConf job) {
      srcs = job.getInt("testdatamerge.sources", 0);
      assertTrue("Invalid src count: " + srcs, srcs > 0);
    }
    public abstract void map(IntWritable key, V val,
        OutputCollector<IntWritable, IntWritable> out, Reporter reporter)
        throws IOException;
    public void reduce(IntWritable key, Iterator<IntWritable> values,
                       OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {
      int seen = 0;
      while (values.hasNext()) {
        seen += values.next().get();
      }
      assertTrue("Bad count for " + key.get(), verify(key.get(), seen));
    }
    public abstract boolean verify(int key, int occ);
  }

  private static class InnerJoinChecker
      extends SimpleCheckerBase<TupleWritable> {
    public void map(IntWritable key, TupleWritable val,
        OutputCollector<IntWritable, IntWritable> out, Reporter reporter)
        throws IOException {
      int k = key.get();
      final String kvstr = "Unexpected tuple: " + stringify(key, val);
      assertTrue(kvstr, 0 == k % (srcs * srcs));
      for (int i = 0; i < val.size(); ++i) {
        final int vali = ((IntWritable)val.get(i)).get();
        assertTrue(kvstr, (vali - i) * srcs == 10 * k);
      }
      out.collect(key, one);
    }
    public boolean verify(int key, int occ) {
      return (key == 0 && occ == 2) ||
             (key != 0 && (key % (srcs * srcs) == 0) && occ == 1);
    }
  }

  private static class OuterJoinChecker
      extends SimpleCheckerBase<TupleWritable> {
    public void map(IntWritable key, TupleWritable val,
        OutputCollector<IntWritable, IntWritable> out, Reporter reporter)
        throws IOException {
      int k = key.get();
      final String kvstr = "Unexpected tuple: " + stringify(key, val);
      if (0 == k % (srcs * srcs)) {
        for (int i = 0; i < val.size(); ++i) {
          assertTrue(kvstr, val.get(i) instanceof IntWritable);
          final int vali = ((IntWritable)val.get(i)).get();
          assertTrue(kvstr, (vali - i) * srcs == 10 * k);
        }
      } else {
        for (int i = 0; i < val.size(); ++i) {
          if (i == k % srcs) {
            assertTrue(kvstr, val.get(i) instanceof IntWritable);
            final int vali = ((IntWritable)val.get(i)).get();
            assertTrue(kvstr, srcs * (vali - i) == 10 * (k - i));
          } else {
            assertTrue(kvstr, !val.has(i));
          }
        }
      }
      out.collect(key, one);
    }
    public boolean verify(int key, int occ) {
      if (key < srcs * srcs && (key % (srcs + 1)) == 0)
        return 2 == occ;
      return 1 == occ;
    }
  }

  private static class OverrideChecker
      extends SimpleCheckerBase<IntWritable> {
    public void map(IntWritable key, IntWritable val,
        OutputCollector<IntWritable, IntWritable> out, Reporter reporter)
        throws IOException {
      int k = key.get();
      final int vali = val.get();
      final String kvstr = "Unexpected tuple: " + stringify(key, val);
      if (0 == k % (srcs * srcs)) {
        assertTrue(kvstr, vali == k * 10 / srcs + srcs - 1);
      } else {
        final int i = k % srcs;
        assertTrue(kvstr, srcs * (vali - i) == 10 * (k - i));
      }
      out.collect(key, one);
    }
    public boolean verify(int key, int occ) {
      if (key < srcs * srcs && (key % (srcs + 1)) == 0 && key != 0)
        return 2 == occ;
      return 1 == occ;
    }
  }

  private static void joinAs(String jointype,
      Class<? extends SimpleCheckerBase> c) throws Exception {
    final int srcs = 4;
    Configuration conf = new Configuration();
    JobConf job = new JobConf(conf, c);
    Path base = cluster.getFileSystem().makeQualified(new Path("/"+jointype));
    Path[] src = writeSimpleSrc(base, conf, srcs);
    job.set("mapred.join.expr", CompositeInputFormat.compose(jointype,
        SequenceFileInputFormat.class, src));
    job.setInt("testdatamerge.sources", srcs);
    job.setInputFormat(CompositeInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(base, "out"));

    job.setMapperClass(c);
    job.setReducerClass(c);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    JobClient.runJob(job);
    base.getFileSystem(job).delete(base, true);
  }

  public void testSimpleInnerJoin() throws Exception {
    joinAs("inner", InnerJoinChecker.class);
  }

  public void testSimpleOuterJoin() throws Exception {
    joinAs("outer", OuterJoinChecker.class);
  }

  public void testSimpleOverride() throws Exception {
    joinAs("override", OverrideChecker.class);
  }

  public void testNestedJoin() throws Exception {
    // outer(inner(S1,...,Sn),outer(S1,...Sn))
    final int SOURCES = 3;
    final int ITEMS = (SOURCES + 1) * (SOURCES + 1);
    JobConf job = new JobConf();
    Path base = cluster.getFileSystem().makeQualified(new Path("/nested"));
    int[][] source = new int[SOURCES][];
    for (int i = 0; i < SOURCES; ++i) {
      source[i] = new int[ITEMS];
      for (int j = 0; j < ITEMS; ++j) {
        source[i][j] = (i + 2) * (j + 1);
      }
    }
    Path[] src = new Path[SOURCES];
    SequenceFile.Writer out[] = createWriters(base, job, SOURCES, src);
    IntWritable k = new IntWritable();
    for (int i = 0; i < SOURCES; ++i) {
      IntWritable v = new IntWritable();
      v.set(i);
      for (int j = 0; j < ITEMS; ++j) {
        k.set(source[i][j]);
        out[i].append(k, v);
      }
      out[i].close();
    }
    out = null;

    StringBuilder sb = new StringBuilder();
    sb.append("outer(inner(");
    for (int i = 0; i < SOURCES; ++i) {
      sb.append(
          CompositeInputFormat.compose(SequenceFileInputFormat.class,
            src[i].toString()));
      if (i + 1 != SOURCES) sb.append(",");
    }
    sb.append("),outer(");
    sb.append(CompositeInputFormat.compose(Fake_IF.class,"foobar"));
    sb.append(",");
    for (int i = 0; i < SOURCES; ++i) {
      sb.append(
          CompositeInputFormat.compose(SequenceFileInputFormat.class,
            src[i].toString()));
      sb.append(",");
    }
    sb.append(CompositeInputFormat.compose(Fake_IF.class,"raboof") + "))");
    job.set("mapred.join.expr", sb.toString());
    job.setInputFormat(CompositeInputFormat.class);
    Path outf = new Path(base, "out");
    FileOutputFormat.setOutputPath(job, outf);
    Fake_IF.setKeyClass(job, IntWritable.class);
    Fake_IF.setValClass(job, IntWritable.class);

    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(IdentityReducer.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(TupleWritable.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    JobClient.runJob(job);

    FileStatus[] outlist = cluster.getFileSystem().listStatus(outf, 
        new Utils.OutputFileUtils.OutputFilesFilter());
    assertEquals(1, outlist.length);
    assertTrue(0 < outlist[0].getLen());
    SequenceFile.Reader r =
      new SequenceFile.Reader(cluster.getFileSystem(),
          outlist[0].getPath(), job);
    TupleWritable v = new TupleWritable();
    while (r.next(k, v)) {
      assertFalse(((TupleWritable)v.get(1)).has(0));
      assertFalse(((TupleWritable)v.get(1)).has(SOURCES + 1));
      boolean chk = true;
      int ki = k.get();
      for (int i = 2; i < SOURCES + 2; ++i) {
        if ((ki % i) == 0 && ki <= i * ITEMS) {
          assertEquals(i - 2, ((IntWritable)
                              ((TupleWritable)v.get(1)).get((i - 1))).get());
        } else chk = false;
      }
      if (chk) { // present in all sources; chk inner
        assertTrue(v.has(0));
        for (int i = 0; i < SOURCES; ++i)
          assertTrue(((TupleWritable)v.get(0)).has(i));
      } else { // should not be present in inner join
        assertFalse(v.has(0));
      }
    }
    r.close();
    base.getFileSystem(job).delete(base, true);

  }

  public void testEmptyJoin() throws Exception {
    JobConf job = new JobConf();
    Path base = cluster.getFileSystem().makeQualified(new Path("/empty"));
    Path[] src = { new Path(base,"i0"), new Path("i1"), new Path("i2") };
    job.set("mapred.join.expr", CompositeInputFormat.compose("outer",
        Fake_IF.class, src));
    job.setInputFormat(CompositeInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(base, "out"));

    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(IdentityReducer.class);
    job.setOutputKeyClass(IncomparableKey.class);
    job.setOutputValueClass(NullWritable.class);

    JobClient.runJob(job);
    base.getFileSystem(job).delete(base, true);
  }

  public static class Fake_IF<K,V>
      implements InputFormat<K,V>, JobConfigurable {

    public static class FakeSplit implements InputSplit {
      public void write(DataOutput out) throws IOException { }
      public void readFields(DataInput in) throws IOException { }
      public long getLength() { return 0L; }
      public String[] getLocations() { return new String[0]; }
    }

    public static void setKeyClass(JobConf job, Class<?> k) {
      job.setClass("test.fakeif.keyclass", k, WritableComparable.class);
    }

    public static void setValClass(JobConf job, Class<?> v) {
      job.setClass("test.fakeif.valclass", v, Writable.class);
    }

    private Class<? extends K> keyclass;
    private Class<? extends V> valclass;

    @SuppressWarnings("unchecked")
    public void configure(JobConf job) {
      keyclass = (Class<? extends K>) job.getClass("test.fakeif.keyclass",
    IncomparableKey.class, WritableComparable.class);
      valclass = (Class<? extends V>) job.getClass("test.fakeif.valclass",
    NullWritable.class, WritableComparable.class);
    }

    public Fake_IF() { }

    public InputSplit[] getSplits(JobConf conf, int splits) {
      return new InputSplit[] { new FakeSplit() };
    }

    public RecordReader<K,V> getRecordReader(
        InputSplit ignored, JobConf conf, Reporter reporter) {
      return new RecordReader<K,V>() {
        public boolean next(K key, V value) throws IOException { return false; }
        public K createKey() {
          return ReflectionUtils.newInstance(keyclass, null);
        }
        public V createValue() {
          return ReflectionUtils.newInstance(valclass, null);
        }
        public long getPos() throws IOException { return 0L; }
        public void close() throws IOException { }
        public float getProgress() throws IOException { return 0.0f; }
      };
    }
  }
}
