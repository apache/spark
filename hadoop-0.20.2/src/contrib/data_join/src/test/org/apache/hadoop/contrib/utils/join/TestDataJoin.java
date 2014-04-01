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

package org.apache.hadoop.contrib.utils.join;

import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.extensions.TestSetup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TestDataJoin extends TestCase {

  private static MiniDFSCluster cluster = null;
  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestDataJoin.class)) {
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

  public void testDataJoin() throws Exception {
    final int srcs = 4;
    JobConf job = new JobConf();
    Path base = cluster.getFileSystem().makeQualified(new Path("/inner"));
    Path[] src = writeSimpleSrc(base, job, srcs);
    job.setInputFormat(SequenceFileInputFormat.class);
    Path outdir = new Path(base, "out");
    FileOutputFormat.setOutputPath(job, outdir);

    job.setMapperClass(SampleDataJoinMapper.class);
    job.setReducerClass(SampleDataJoinReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(SampleTaggedMapOutput.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setNumMapTasks(1);
    job.setNumReduceTasks(1);
    FileInputFormat.setInputPaths(job, src);
    try {
      JobClient.runJob(job);
      confirmOutput(outdir, job, srcs);
    } finally {
      base.getFileSystem(job).delete(base, true);
    }
  }

  private static void confirmOutput(Path out, JobConf job, int srcs)
      throws IOException {
    FileSystem fs = out.getFileSystem(job);
    FileStatus[] outlist = fs.listStatus(out);
    assertEquals(1, outlist.length);
    assertTrue(0 < outlist[0].getLen());
    FSDataInputStream in = fs.open(outlist[0].getPath());
    LineRecordReader rr = new LineRecordReader(in, 0, Integer.MAX_VALUE, job);
    LongWritable k = new LongWritable();
    Text v = new Text();
    int count = 0;
    while (rr.next(k, v)) {
      String[] vals = v.toString().split("\t");
      assertEquals(srcs + 1, vals.length);
      int[] ivals = new int[vals.length];
      for (int i = 0; i < vals.length; ++i)
        ivals[i] = Integer.parseInt(vals[i]);
      assertEquals(0, ivals[0] % (srcs * srcs));
      for (int i = 1; i < vals.length; ++i) {
        assertEquals((ivals[i] - (i - 1)) * srcs, 10 * ivals[0]);
      }
      ++count;
    }
    assertEquals(4, count);
  }

  private static SequenceFile.Writer[] createWriters(Path testdir,
      JobConf conf, int srcs, Path[] src) throws IOException {
    for (int i = 0; i < srcs; ++i) {
      src[i] = new Path(testdir, Integer.toString(i + 10, 36));
    }
    SequenceFile.Writer out[] = new SequenceFile.Writer[srcs];
    for (int i = 0; i < srcs; ++i) {
      out[i] = new SequenceFile.Writer(testdir.getFileSystem(conf), conf,
          src[i], Text.class, Text.class);
    }
    return out;
  }

  private static Path[] writeSimpleSrc(Path testdir, JobConf conf,
      int srcs) throws IOException {
    SequenceFile.Writer out[] = null;
    Path[] src = new Path[srcs];
    try {
      out = createWriters(testdir, conf, srcs, src);
      final int capacity = srcs * 2 + 1;
      Text key = new Text();
      key.set("ignored");
      Text val = new Text();
      for (int k = 0; k < capacity; ++k) {
        for (int i = 0; i < srcs; ++i) {
          val.set(Integer.toString(k % srcs == 0 ? k * srcs : k * srcs + i) +
              "\t" + Integer.toString(10 * k + i));
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
}
