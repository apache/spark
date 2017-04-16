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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.*;
import java.util.*;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TestNLineInputFormat extends TestCase {
  private static int MAX_LENGTH = 200;
  
  private static Configuration conf = new Configuration();
  private static FileSystem localFs = null; 

  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  private static Path workDir = 
    new Path(new Path(System.getProperty("test.build.data", "."), "data"),
             "TestNLineInputFormat");
  
  public void testFormat() throws Exception {
    Job job = new Job(conf);
    Path file = new Path(workDir, "test.txt");

    int seed = new Random().nextInt();
    Random random = new Random(seed);

    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, workDir);
    int numLinesPerMap = 5;
    NLineInputFormat.setNumLinesPerSplit(job, numLinesPerMap);
    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length += random.nextInt(MAX_LENGTH / 10) + 1) {
      // create a file with length entries
      Writer writer = new OutputStreamWriter(localFs.create(file));
      try {
        for (int i = 0; i < length; i++) {
          writer.write(Integer.toString(i));
          writer.write("\n");
        }
      } finally {
        writer.close();
      }
      checkFormat(job, numLinesPerMap);
    }
  }

  void checkFormat(Job job, int expectedN) 
      throws IOException, InterruptedException {
    NLineInputFormat format = new NLineInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    // check all splits except last one
    int count = 0;
    for (int i = 0; i < splits.size() -1; i++) {
      assertEquals("There are no split locations", 0,
                   splits.get(i).getLocations().length);
      TaskAttemptContext context = MapReduceTestUtil.
        createDummyMapTaskAttemptContext(job.getConfiguration());
      RecordReader<LongWritable, Text> reader = format.createRecordReader(
        splits.get(i), context);
      Class<?> clazz = reader.getClass();
      assertEquals("reader class is LineRecordReader.", 
        LineRecordReader.class, clazz);
      MapContext<LongWritable, Text, LongWritable, Text> mcontext = 
        new MapContext<LongWritable, Text, LongWritable, Text>(
          job.getConfiguration(), context.getTaskAttemptID(), reader, null,
          null, MapReduceTestUtil.createDummyReporter(), splits.get(i));
      reader.initialize(splits.get(i), mcontext);
         
      try {
        count = 0;
        while (reader.nextKeyValue()) {
          count++;
        }
      } finally {
        reader.close();
      }
      assertEquals("number of lines in split is " + expectedN ,
                   expectedN, count);
    }
  }
  
  public static void main(String[] args) throws Exception {
    new TestNLineInputFormat().testFormat();
  }
}
