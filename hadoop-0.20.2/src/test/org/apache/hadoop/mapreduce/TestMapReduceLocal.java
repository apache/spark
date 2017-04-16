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

package org.apache.hadoop.mapreduce;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.MultiFileWordCount;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.examples.WordCount.TokenizerMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * A JUnit test to test min map-reduce cluster with local file system.
 */
public class TestMapReduceLocal extends TestCase {
  private static Path TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"));
  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  public static Path writeFile(String name, String data) throws IOException {
    Path file = new Path(TEST_ROOT_DIR + "/" + name);
    localFs.delete(file, false);
    DataOutputStream f = localFs.create(file);
    f.write(data.getBytes());
    f.close();
    return file;
  }

  public static String readFile(String name) throws IOException {
    DataInputStream f = localFs.open(new Path(TEST_ROOT_DIR + "/" + name));
    BufferedReader b = new BufferedReader(new InputStreamReader(f));
    StringBuilder result = new StringBuilder();
    String line = b.readLine();
    while (line != null) {
     result.append(line);
     result.append('\n');
     line = b.readLine();
    }
    b.close();
    return result.toString();
  }

  public void testWithLocal() throws Exception {
    MiniMRCluster mr = null;
    try {
      mr = new MiniMRCluster(2, "file:///", 3);
      Configuration conf = mr.createJobConf();
      runWordCount(conf);
      runMultiFileWordCount(conf);
    } finally {
      if (mr != null) { mr.shutdown(); }
    }
  }

  public static class TrackingTextInputFormat extends TextInputFormat {

    public static class MonoProgressRecordReader extends LineRecordReader {
      private float last = 0.0f;
      private boolean progressCalled = false;
      @Override
      public float getProgress() {
        progressCalled = true;
        final float ret = super.getProgress();
        assertTrue("getProgress decreased", ret >= last);
        last = ret;
        return ret;
      }
      @Override
      public synchronized void close() throws IOException {
        assertTrue("getProgress never called", progressCalled);
        super.close();
      }
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
        InputSplit split, TaskAttemptContext context) {
      return new MonoProgressRecordReader();
    }
  }

  private void runWordCount(Configuration conf
                            ) throws IOException,
                                     InterruptedException,
                                     ClassNotFoundException {
    final String COUNTER_GROUP = "org.apache.hadoop.mapred.Task$Counter";
    localFs.delete(new Path(TEST_ROOT_DIR + "/in"), true);
    localFs.delete(new Path(TEST_ROOT_DIR + "/out"), true);    
    writeFile("in/part1", "this is a test\nof word count test\ntest\n");
    writeFile("in/part2", "more test");
    Job job = new Job(conf, "word count");     
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(TrackingTextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(TEST_ROOT_DIR + "/in"));
    FileOutputFormat.setOutputPath(job, new Path(TEST_ROOT_DIR + "/out"));
    assertNull(job.getJobID());
    assertTrue(job.waitForCompletion(false));
    assertNotNull(job.getJobID());
    String out = readFile("out/part-r-00000");
    System.out.println(out);
    assertEquals("a\t1\ncount\t1\nis\t1\nmore\t1\nof\t1\ntest\t4\nthis\t1\nword\t1\n",
                 out);
    Counters ctrs = job.getCounters();
    System.out.println("Counters: " + ctrs);
    long combineIn = ctrs.findCounter(COUNTER_GROUP,
                                      "COMBINE_INPUT_RECORDS").getValue();
    long combineOut = ctrs.findCounter(COUNTER_GROUP, 
                                       "COMBINE_OUTPUT_RECORDS").getValue();
    long reduceIn = ctrs.findCounter(COUNTER_GROUP,
                                     "REDUCE_INPUT_RECORDS").getValue();
    long mapOut = ctrs.findCounter(COUNTER_GROUP, 
                                   "MAP_OUTPUT_RECORDS").getValue();
    long reduceOut = ctrs.findCounter(COUNTER_GROUP,
                                      "REDUCE_OUTPUT_RECORDS").getValue();
    long reduceGrps = ctrs.findCounter(COUNTER_GROUP,
                                       "REDUCE_INPUT_GROUPS").getValue();
    assertEquals("map out = combine in", mapOut, combineIn);
    assertEquals("combine out = reduce in", combineOut, reduceIn);
    assertTrue("combine in > combine out", combineIn > combineOut);
    assertEquals("reduce groups = reduce out", reduceGrps, reduceOut);
    String group = "Random Group";
    CounterGroup ctrGrp = ctrs.getGroup(group);
    assertEquals(0, ctrGrp.size());
  }
 
  public void runMultiFileWordCount(Configuration  conf) throws Exception  {
    localFs.delete(new Path(TEST_ROOT_DIR + "/in"), true);
    localFs.delete(new Path(TEST_ROOT_DIR + "/out"), true);    
    writeFile("in/part1", "this is a test\nof " +
              "multi file word count test\ntest\n");
    writeFile("in/part2", "more test");

    int ret = ToolRunner.run(conf, new MultiFileWordCount(), 
                new String[] {TEST_ROOT_DIR + "/in", TEST_ROOT_DIR + "/out"});
    assertTrue("MultiFileWordCount failed", ret == 0);
    String out = readFile("out/part-r-00000");
    System.out.println(out);
    assertEquals("a\t1\ncount\t1\nfile\t1\nis\t1\n" +
      "more\t1\nmulti\t1\nof\t1\ntest\t4\nthis\t1\nword\t1\n", out);
  }

}
