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
package org.apache.hadoop.mapreduce.split;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/**
 * A JUnit test to test limits on block locations
 */
public class TestBlockLimits extends TestCase {
  private static String TEST_ROOT_DIR =
    new File(System.getProperty("test.build.data","/tmp"))
    .toURI().toString().replace(' ', '+');
    
  public void testWithLimits()
      throws IOException, InterruptedException, ClassNotFoundException {
    MiniMRCluster mr = null;
    try {
      mr = new MiniMRCluster(2, "file:///", 3);
      Configuration conf = new Configuration();
      conf.setInt(JobSplitWriter.MAX_SPLIT_LOCATIONS, 10);
      mr = new MiniMRCluster(2, "file:///", 3, null, null, new JobConf(conf));
      runCustomFormat(mr);
    } finally {
      if (mr != null) { mr.shutdown(); }
    }
  }
  
  private void runCustomFormat(MiniMRCluster mr) throws IOException {
    JobConf job = new JobConf(mr.createJobConf());
    job.setInt(JobSplitWriter.MAX_SPLIT_LOCATIONS, 100);
    FileSystem fileSys = FileSystem.get(job);
    Path testDir = new Path(TEST_ROOT_DIR + "/test_mini_mr_local");
    Path outDir = new Path(testDir, "out");
    System.out.println("testDir= " + testDir);
    fileSys.delete(testDir, true);
    job.setInputFormat(MyInputFormat.class);
    job.setOutputFormat(MyOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.setMapperClass(MyMapper.class);        
    job.setNumReduceTasks(0);
    job.set("non.std.out", outDir.toString());
    try {
      JobClient.runJob(job);
      fail("JobTracker neglected to fail misconfigured job");
    } catch(IOException ie) {
      System.out.println("Failed job " + StringUtils.stringifyException(ie));
    } finally {
      fileSys.delete(testDir, true);
    }
    
  }
  
  static class MyMapper extends MapReduceBase
    implements Mapper<WritableComparable, Writable,
                    WritableComparable, Writable> {

    public void map(WritableComparable key, Writable value,
                  OutputCollector<WritableComparable, Writable> out,
                  Reporter reporter) throws IOException {
    }
  }

  private static class MyInputFormat
    implements InputFormat<Text, Text> {
    
    private static class MySplit implements InputSplit {
      int first;
      int length;

      public MySplit() { }

      public MySplit(int first, int length) {
        this.first = first;
        this.length = length;
      }

      public String[] getLocations() {
        final String[] ret = new String[200];
        Arrays.fill(ret, "SPLIT");
        return ret;
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
    
    public InputSplit[] getSplits(JobConf job, 
                                  int numSplits) throws IOException {
      return new MySplit[]{new MySplit(0, 1), new MySplit(1, 3),
                           new MySplit(4, 2)};
    }

    public RecordReader<Text, Text> getRecordReader(InputSplit split,
                                                           JobConf job, 
                                                           Reporter reporter)
                                                           throws IOException {
      MySplit sp = (MySplit) split;
      return new RecordReader<Text,Text>() {
        @Override public boolean next(Text key, Text value) { return false; }
        @Override public Text createKey() { return new Text(); }
        @Override public Text createValue() { return new Text(); }
        @Override public long getPos() throws IOException { return 0; }
        @Override public void close() throws IOException { }
        @Override public float getProgress() throws IOException { return 1.0f; }
      };
    }
    
  }
  

  static class MyOutputFormat implements OutputFormat {
    static class MyRecordWriter implements RecordWriter<Object, Object> {
      private DataOutputStream out;
      
      public MyRecordWriter(Path outputFile, JobConf job) throws IOException {
      }
      
      public void write(Object key, Object value) throws IOException {
        return;
      }

      public void close(Reporter reporter) throws IOException {
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

}
