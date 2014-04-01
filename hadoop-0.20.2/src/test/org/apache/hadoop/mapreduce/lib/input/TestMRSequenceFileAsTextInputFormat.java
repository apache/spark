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

import java.util.*;
import junit.framework.TestCase;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.conf.*;

public class TestMRSequenceFileAsTextInputFormat extends TestCase {
  private static int MAX_LENGTH = 10000;
  private static Configuration conf = new Configuration();

  public void testFormat() throws Exception {
    Job job = new Job(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(System.getProperty("test.build.data",".") + "/mapred");
    Path file = new Path(dir, "test.seq");
    
    int seed = new Random().nextInt();
    Random random = new Random(seed);

    fs.delete(dir, true);

    FileInputFormat.setInputPaths(job, dir);

    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length += random.nextInt(MAX_LENGTH / 10) + 1) {

      // create a file with length entries
      SequenceFile.Writer writer =
        SequenceFile.createWriter(fs, conf, file,
          IntWritable.class, LongWritable.class);
      try {
        for (int i = 0; i < length; i++) {
          IntWritable key = new IntWritable(i);
          LongWritable value = new LongWritable(10 * i);
          writer.append(key, value);
        }
      } finally {
        writer.close();
      }

      TaskAttemptContext context = MapReduceTestUtil.
        createDummyMapTaskAttemptContext(job.getConfiguration());
      // try splitting the file in a variety of sizes
      InputFormat<Text, Text> format =
        new SequenceFileAsTextInputFormat();
      
      for (int i = 0; i < 3; i++) {
        // check each split
        BitSet bits = new BitSet(length);
        int numSplits =
          random.nextInt(MAX_LENGTH / (SequenceFile.SYNC_INTERVAL / 20)) + 1;
        FileInputFormat.setMaxInputSplitSize(job, 
          fs.getFileStatus(file).getLen() / numSplits);
        for (InputSplit split : format.getSplits(job)) {
          RecordReader<Text, Text> reader =
            format.createRecordReader(split, context);
          MapContext<Text, Text, Text, Text> mcontext = 
            new MapContext<Text, Text, Text, Text>(job.getConfiguration(), 
            context.getTaskAttemptID(), reader, null, null, 
            MapReduceTestUtil.createDummyReporter(), 
            split);
          reader.initialize(split, mcontext);
          Class<?> readerClass = reader.getClass();
          assertEquals("reader class is SequenceFileAsTextRecordReader.",
            SequenceFileAsTextRecordReader.class, readerClass);        
          Text key;
          try {
            int count = 0;
            while (reader.nextKeyValue()) {
              key = reader.getCurrentKey();
              int keyInt = Integer.parseInt(key.toString());
              assertFalse("Key in multiple partitions.", bits.get(keyInt));
              bits.set(keyInt);
              count++;
            }
          } finally {
            reader.close();
          }
        }
        assertEquals("Some keys in no partition.", length, bits.cardinality());
      }

    }
  }

  public static void main(String[] args) throws Exception {
    new TestMRSequenceFileAsTextInputFormat().testFormat();
  }
}
