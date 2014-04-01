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
package org.apache.hadoop.mapreduce.lib.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapreduce.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TestMRMultipleOutputs extends HadoopTestCase {

  public TestMRMultipleOutputs() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  public void testWithoutCounters() throws Exception {
    _testMultipleOutputs(false);
  }

  public void testWithCounters() throws Exception {
    _testMultipleOutputs(true);
  }

  private static String localPathRoot = 
    System.getProperty("test.build.data", "/tmp");
  private static final Path ROOT_DIR = new Path(localPathRoot, "testing/mo");
  private static final Path IN_DIR = new Path(ROOT_DIR, "input");
  private static final Path OUT_DIR = new Path(ROOT_DIR, "output");
  private static String TEXT = "text";
  private static String SEQUENCE = "sequence";

  public void setUp() throws Exception {
    super.setUp();
    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    fs.delete(ROOT_DIR, true);
  }

  public void tearDown() throws Exception {
    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    fs.delete(ROOT_DIR, true);
    super.tearDown();
  }

  protected void _testMultipleOutputs(boolean withCounters) throws Exception {
    String input = "a\nb\nc\nd\ne\nc\nd\ne";

    Configuration conf = createJobConf();
    Job job = MapReduceTestUtil.createJob(conf, IN_DIR, OUT_DIR, 2, 1, input);

    job.setJobName("mo");
    MultipleOutputs.addNamedOutput(job, TEXT, TextOutputFormat.class,
      LongWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, SEQUENCE,
      SequenceFileOutputFormat.class, IntWritable.class, Text.class);

    MultipleOutputs.setCountersEnabled(job, withCounters);

    job.setMapperClass(MOMap.class);
    job.setReducerClass(MOReduce.class);

    job.waitForCompletion(true);

    // assert number of named output part files
    int namedOutputCount = 0;
    int valueBasedOutputCount = 0;
    FileSystem fs = OUT_DIR.getFileSystem(conf);
    FileStatus[] statuses = fs.listStatus(OUT_DIR);
    for (FileStatus status : statuses) {
      String fileName = status.getPath().getName();
      if (fileName.equals("text-m-00000") ||
          fileName.equals("text-m-00001") ||
          fileName.equals("text-r-00000") ||
          fileName.equals("sequence_A-m-00000") ||
          fileName.equals("sequence_A-m-00001") ||
          fileName.equals("sequence_B-m-00000") ||
          fileName.equals("sequence_B-m-00001") ||
          fileName.equals("sequence_B-r-00000") ||
          fileName.equals("sequence_C-r-00000")) {
        namedOutputCount++;
      } else if (fileName.equals("a-r-00000") ||
          fileName.equals("b-r-00000") ||
          fileName.equals("c-r-00000") ||
          fileName.equals("d-r-00000") ||
          fileName.equals("e-r-00000")) {
        valueBasedOutputCount++;
      }
    }
    assertEquals(9, namedOutputCount);
    assertEquals(5, valueBasedOutputCount);

    // assert TextOutputFormat files correctness
    BufferedReader reader = new BufferedReader(
      new InputStreamReader(fs.open(
        new Path(FileOutputFormat.getOutputPath(job), "text-r-00000"))));
    int count = 0;
    String line = reader.readLine();
    while (line != null) {
      assertTrue(line.endsWith(TEXT));
      line = reader.readLine();
      count++;
    }
    reader.close();
    assertFalse(count == 0);
    
    // assert SequenceOutputFormat files correctness
    SequenceFile.Reader seqReader =
      new SequenceFile.Reader(fs, new Path(FileOutputFormat.getOutputPath(job),
        "sequence_B-r-00000"), conf);

    assertEquals(IntWritable.class, seqReader.getKeyClass());
    assertEquals(Text.class, seqReader.getValueClass());

    count = 0;
    IntWritable key = new IntWritable();
    Text value = new Text();
    while (seqReader.next(key, value)) {
      assertEquals(SEQUENCE, value.toString());
      count++;
    }
    seqReader.close();
    assertFalse(count == 0);

    if (withCounters) {
      CounterGroup counters =
        job.getCounters().getGroup(MultipleOutputs.class.getName());
      assertEquals(9, counters.size());
      assertEquals(4, counters.findCounter(TEXT).getValue());
      assertEquals(2, counters.findCounter(SEQUENCE + "_A").getValue());
      assertEquals(4, counters.findCounter(SEQUENCE + "_B").getValue());
      assertEquals(2, counters.findCounter(SEQUENCE + "_C").getValue());
      assertEquals(2, counters.findCounter("a").getValue());
      assertEquals(2, counters.findCounter("b").getValue());
      assertEquals(4, counters.findCounter("c").getValue());
      assertEquals(4, counters.findCounter("d").getValue());
      assertEquals(4, counters.findCounter("e").getValue());
    }
  }

  @SuppressWarnings({"unchecked"})
  public static class MOMap extends Mapper<LongWritable, Text, LongWritable,
    Text> {

    private MultipleOutputs mos;

    public void setup(Context context) {
      mos = new MultipleOutputs(context);
    }

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
      if (value.toString().equals("a")) {
        mos.write(TEXT, key, new Text(TEXT));
        mos.write(SEQUENCE, new IntWritable(1), new Text(SEQUENCE),
          (SEQUENCE + "_A"));
        mos.write(SEQUENCE, new IntWritable(2), new Text(SEQUENCE),
          (SEQUENCE + "_B"));
      }
    }

    public void cleanup(Context context) 
        throws IOException, InterruptedException {
      mos.close();
    }
  }

  @SuppressWarnings({"unchecked"})
  public static class MOReduce extends Reducer<LongWritable, Text,
    LongWritable, Text> {

    private MultipleOutputs mos;
    
    public void setup(Context context) {
      mos = new MultipleOutputs(context);
   }

    public void reduce(LongWritable key, Iterable<Text> values, 
        Context context) throws IOException, InterruptedException {
      for (Text value : values) {
        mos.write(key, value, value.toString());
        if (!value.toString().equals("b")) {
          context.write(key, value);
        } else {
          mos.write(TEXT, key, new Text(TEXT));
          mos.write(SEQUENCE, new IntWritable(2), new Text(SEQUENCE),
            (SEQUENCE + "_B"));
          mos.write(SEQUENCE, new IntWritable(3), new Text(SEQUENCE),
            (SEQUENCE + "_C"));
        }
      }
    }

    public void cleanup(Context context) 
        throws IOException, InterruptedException {
      mos.close();
    }
  }
}
