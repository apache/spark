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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SecondarySort.FirstGroupingComparator;
import org.apache.hadoop.examples.SecondarySort.FirstPartitioner;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.examples.SecondarySort;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.MRCaching.TestResult;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.TestMapReduceLocal;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * A JUnit test to test min map-reduce cluster with local file system.
 */
public class TestMiniMRLocalFS extends TestCase {
  private static String TEST_ROOT_DIR =
    new File(System.getProperty("test.build.data","/tmp"))
    .toURI().toString().replace(' ', '+');
    
  public void testWithLocal()
      throws IOException, InterruptedException, ClassNotFoundException {
    MiniMRCluster mr = null;
    try {
      mr = new MiniMRCluster(2, "file:///", 3);
      // make cleanup inline sothat validation of existence of these directories
      // can be done
      mr.setInlineCleanupThreads();

      TestMiniMRWithDFS.runPI(mr, mr.createJobConf());

      // run the wordcount example with caching
      JobConf job = mr.createJobConf();
      TestResult ret = MRCaching.launchMRCache(TEST_ROOT_DIR + "/wc/input",
                                            TEST_ROOT_DIR + "/wc/output", 
                                            TEST_ROOT_DIR + "/cachedir",
                                            job,
                                            "The quick brown fox\n" 
                                            + "has many silly\n"
                                            + "red fox sox\n");
      // assert the number of lines read during caching
      assertTrue("Failed test archives not matching", ret.isOutputOk);
      // test the task report fetchers
      JobClient client = new JobClient(job);
      JobID jobid = ret.job.getID();
      TaskReport[] reports;
      reports = client.getSetupTaskReports(jobid);
      assertEquals("number of setups", 2, reports.length);
      reports = client.getMapTaskReports(jobid);
      assertEquals("number of maps", 1, reports.length);
      reports = client.getReduceTaskReports(jobid);
      assertEquals("number of reduces", 1, reports.length);
      reports = client.getCleanupTaskReports(jobid);
      assertEquals("number of cleanups", 2, reports.length);
      Counters counters = ret.job.getCounters();
      assertEquals("number of map inputs", 3, 
                   counters.getCounter(Task.Counter.MAP_INPUT_RECORDS));
      assertEquals("number of reduce outputs", 9, 
                   counters.getCounter(Task.Counter.REDUCE_OUTPUT_RECORDS));
      runCustomFormats(mr);
      runSecondarySort(mr.createJobConf());
    } finally {
      if (mr != null) { mr.shutdown(); }
    }
  }
  
  private void runCustomFormats(MiniMRCluster mr) throws IOException {
    JobConf job = mr.createJobConf();
    FileSystem fileSys = FileSystem.get(job);
    Path testDir = new Path(TEST_ROOT_DIR + "/test_mini_mr_local");
    Path outDir = new Path(testDir, "out");
    System.out.println("testDir= " + testDir);
    fileSys.delete(testDir, true);
    
    job.setInputFormat(MyInputFormat.class);
    job.setOutputFormat(MyOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setMapperClass(MyMapper.class);        
    job.setReducerClass(MyReducer.class);
    job.setNumMapTasks(100);
    job.setNumReduceTasks(1);
    // explicitly do not use "normal" job.setOutputPath to make sure
    // that it is not hardcoded anywhere in the framework.
    job.set("non.std.out", outDir.toString());
    try {
      JobClient.runJob(job);
      String result = 
        MapReduceTestUtil.readOutput(outDir, job);
      assertEquals("output", ("aunt annie\t1\n" +
                              "bumble boat\t4\n" +
                              "crocodile pants\t0\n" +
                              "duck-dog\t5\n"+
                              "eggs\t2\n" + 
                              "finagle the agent\t3\n"), result);
    } finally {
      fileSys.delete(testDir, true);
    }
    
  }
  
  private static class MyInputFormat
    implements InputFormat<IntWritable, Text> {
    
    static final String[] data = new String[]{
      "crocodile pants", 
      "aunt annie", 
      "eggs",
      "finagle the agent",
      "bumble boat", 
      "duck-dog",
    };

    private static class MySplit implements InputSplit {
      int first;
      int length;

      public MySplit() { }

      public MySplit(int first, int length) {
        this.first = first;
        this.length = length;
      }

      public String[] getLocations() {
        return new String[0];
      }

      public long getLength() {
        return length;
      }

      public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, first);
        WritableUtils.writeVInt(out, length);
      }

      public void readFields(DataInput in) throws IOException {
        first = WritableUtils.readVInt(in);
        length = WritableUtils.readVInt(in);
      }
    }

    static class MyRecordReader implements RecordReader<IntWritable, Text> {
      int index;
      int past;
      int length;
      
      MyRecordReader(int index, int length) {
        this.index = index;
        this.past = index + length;
        this.length = length;
      }

      public boolean next(IntWritable key, Text value) throws IOException {
        if (index < past) {
          key.set(index);
          value.set(data[index]);
          index += 1;
          return true;
        }
        return false;
      }
      
      public IntWritable createKey() {
        return new IntWritable();
      }
      
      public Text createValue() {
        return new Text();
      }

      public long getPos() throws IOException {
        return index;
      }

      public void close() throws IOException {}

      public float getProgress() throws IOException {
        return 1.0f - (past-index)/length;
      }
    }
    
    public InputSplit[] getSplits(JobConf job, 
                                  int numSplits) throws IOException {
      return new MySplit[]{new MySplit(0, 1), new MySplit(1, 3),
                           new MySplit(4, 2)};
    }

    public RecordReader<IntWritable, Text> getRecordReader(InputSplit split,
                                                           JobConf job, 
                                                           Reporter reporter)
                                                           throws IOException {
      MySplit sp = (MySplit) split;
      return new MyRecordReader(sp.first, sp.length);
    }
    
  }
  
  static class MyMapper extends MapReduceBase
    implements Mapper<WritableComparable, Writable,
                      WritableComparable, Writable> {
    
    public void map(WritableComparable key, Writable value, 
                    OutputCollector<WritableComparable, Writable> out,
                    Reporter reporter) throws IOException {
      System.out.println("map: " + key + ", " + value);
      out.collect((WritableComparable) value, key);
      InputSplit split = reporter.getInputSplit();
      if (split.getClass() != MyInputFormat.MySplit.class) {
        throw new IOException("Got wrong split in MyMapper! " + 
                              split.getClass().getName());
      }
    }
  }

  static class MyReducer extends MapReduceBase
    implements Reducer<WritableComparable, Writable,
                      WritableComparable, Writable> {
    public void reduce(WritableComparable key, Iterator<Writable> values, 
                       OutputCollector<WritableComparable, Writable> output,
                       Reporter reporter) throws IOException {
      try {
        InputSplit split = reporter.getInputSplit();
        throw new IOException("Got an input split of " + split);
      } catch (UnsupportedOperationException e) {
        // expected result
      }
      while (values.hasNext()) {
        Writable value = values.next();
        System.out.println("reduce: " + key + ", " + value);
        output.collect(key, value);
      }
    }
  }

  static class MyOutputFormat implements OutputFormat {
    static class MyRecordWriter implements RecordWriter<Object, Object> {
      private DataOutputStream out;
      
      public MyRecordWriter(Path outputFile, JobConf job) throws IOException {
        out = outputFile.getFileSystem(job).create(outputFile);
      }
      
      public void write(Object key, Object value) throws IOException {
        out.writeBytes(key.toString() + "\t" + value.toString() + "\n");
      }

      public void close(Reporter reporter) throws IOException { 
        out.close();
      }
    }
    
    public RecordWriter getRecordWriter(FileSystem ignored, JobConf job, 
                                        String name,
                                        Progressable progress
                                        ) throws IOException {
      return new MyRecordWriter(new Path(job.get("non.std.out")), job);
    }

    public void checkOutputSpecs(FileSystem ignored, 
                                 JobConf job) throws IOException {
    }
  }

  private void runSecondarySort(Configuration conf) throws IOException,
                                                        InterruptedException,
                                                        ClassNotFoundException {
    FileSystem localFs = FileSystem.getLocal(conf);
    localFs.delete(new Path(TEST_ROOT_DIR + "/in"), true);
    localFs.delete(new Path(TEST_ROOT_DIR + "/out"), true);
    TestMapReduceLocal.writeFile
             ("in/part1", "-1 -4\n-3 23\n5 10\n-1 -2\n-1 300\n-1 10\n4 1\n" +
              "4 2\n4 10\n4 -1\n4 -10\n10 20\n10 30\n10 25\n");
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setNumReduceTasks(2);
    job.setMapperClass(SecondarySort.MapClass.class);
    job.setReducerClass(SecondarySort.Reduce.class);
    // group and partition by the first int in the pair
    job.setPartitionerClass(FirstPartitioner.class);
    job.setGroupingComparatorClass(FirstGroupingComparator.class);

    // the map output is IntPair, IntWritable
    job.setMapOutputKeyClass(IntPair.class);
    job.setMapOutputValueClass(IntWritable.class);

    // the reduce output is Text, IntWritable
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(TEST_ROOT_DIR + "/in"));
    FileOutputFormat.setOutputPath(job, new Path(TEST_ROOT_DIR + "/out"));
    assertTrue(job.waitForCompletion(true));
    String out = TestMapReduceLocal.readFile("out/part-r-00000");
    assertEquals("------------------------------------------------\n" +
                 "4\t-10\n4\t-1\n4\t1\n4\t2\n4\t10\n" +
                 "------------------------------------------------\n" +
                 "10\t20\n10\t25\n10\t30\n", out);
    out = TestMapReduceLocal.readFile("out/part-r-00001");
    assertEquals("------------------------------------------------\n" +
                 "-3\t23\n" +
                 "------------------------------------------------\n" +
                 "-1\t-4\n-1\t-2\n-1\t10\n-1\t300\n" +
                 "------------------------------------------------\n" +
                 "5\t10\n", out);
  }
}
