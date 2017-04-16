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

package org.apache.hadoop.record;

import java.io.*;
import java.util.*;
import junit.framework.TestCase;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

public class TestRecordWritable extends TestCase {
  private static final Log LOG = FileInputFormat.LOG;

  private static int MAX_LENGTH = 10000;
  private static Configuration conf = new Configuration();

  public void testFormat() throws Exception {
    JobConf job = new JobConf(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(System.getProperty("test.build.data",".") + "/mapred");
    Path file = new Path(dir, "test.seq");
    
    int seed = new Random().nextInt();
    //LOG.info("seed = "+seed);
    Random random = new Random(seed);

    fs.delete(dir, true);

    FileInputFormat.setInputPaths(job, dir);

    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length+= random.nextInt(MAX_LENGTH/10)+1) {

      // create a file with length entries
      SequenceFile.Writer writer =
        new SequenceFile.Writer(fs, conf, file,
                                RecInt.class, RecBuffer.class);
      try {
        for (int i = 0; i < length; i++) {
          RecInt key = new RecInt();
          key.setData(i);
          byte[] data = new byte[random.nextInt(10)];
          random.nextBytes(data);
          RecBuffer value = new RecBuffer();
          value.setData(new Buffer(data));
          writer.append(key, value);
        }
      } finally {
        writer.close();
      }

      // try splitting the file in a variety of sizes
      InputFormat<RecInt, RecBuffer> format =
        new SequenceFileInputFormat<RecInt, RecBuffer>();
      RecInt key = new RecInt();
      RecBuffer value = new RecBuffer();
      for (int i = 0; i < 3; i++) {
        int numSplits =
          random.nextInt(MAX_LENGTH/(SequenceFile.SYNC_INTERVAL/20))+1;
        InputSplit[] splits = format.getSplits(job, numSplits);

        // check each split
        BitSet bits = new BitSet(length);
        for (int j = 0; j < splits.length; j++) {
          RecordReader<RecInt, RecBuffer> reader =
            format.getRecordReader(splits[j], job, Reporter.NULL);
          try {
            int count = 0;
            while (reader.next(key, value)) {
              assertFalse("Key in multiple partitions.", bits.get(key.getData()));
              bits.set(key.getData());
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
    new TestRecordWritable().testFormat();
  }
}
