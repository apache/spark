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

import java.io.*;
import java.util.*;
import junit.framework.TestCase;

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;

public class TestSequenceFileAsTextInputFormat extends TestCase {
  private static final Log LOG = FileInputFormat.LOG;

  private static int MAX_LENGTH = 10000;
  private static Configuration conf = new Configuration();

  public void testFormat() throws Exception {
    JobConf job = new JobConf(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(System.getProperty("test.build.data",".") + "/mapred");
    Path file = new Path(dir, "test.seq");
    
    Reporter reporter = Reporter.NULL;
    
    int seed = new Random().nextInt();
    //LOG.info("seed = "+seed);
    Random random = new Random(seed);

    fs.delete(dir, true);

    FileInputFormat.setInputPaths(job, dir);

    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length+= random.nextInt(MAX_LENGTH/10)+1) {

      //LOG.info("creating; entries = " + length);

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

      // try splitting the file in a variety of sizes
      InputFormat<Text, Text> format =
        new SequenceFileAsTextInputFormat();
      
      for (int i = 0; i < 3; i++) {
        int numSplits =
          random.nextInt(MAX_LENGTH/(SequenceFile.SYNC_INTERVAL/20))+1;
        //LOG.info("splitting: requesting = " + numSplits);
        InputSplit[] splits = format.getSplits(job, numSplits);
        //LOG.info("splitting: got =        " + splits.length);

        // check each split
        BitSet bits = new BitSet(length);
        for (int j = 0; j < splits.length; j++) {
          RecordReader<Text, Text> reader =
            format.getRecordReader(splits[j], job, reporter);
          Class readerClass = reader.getClass();
          assertEquals("reader class is SequenceFileAsTextRecordReader.", SequenceFileAsTextRecordReader.class, readerClass);        
          Text value = reader.createValue();
          Text key = reader.createKey();
          try {
            int count = 0;
            while (reader.next(key, value)) {
              // if (bits.get(key.get())) {
              // LOG.info("splits["+j+"]="+splits[j]+" : " + key.get());
              // LOG.info("@"+reader.getPos());
              // }
              int keyInt = Integer.parseInt(key.toString());
              assertFalse("Key in multiple partitions.", bits.get(keyInt));
              bits.set(keyInt);
              count++;
            }
            //LOG.info("splits["+j+"]="+splits[j]+" count=" + count);
          } finally {
            reader.close();
          }
        }
        assertEquals("Some keys in no partition.", length, bits.cardinality());
      }

    }
  }

  public static void main(String[] args) throws Exception {
    new TestSequenceFileAsTextInputFormat().testFormat();
  }
}
