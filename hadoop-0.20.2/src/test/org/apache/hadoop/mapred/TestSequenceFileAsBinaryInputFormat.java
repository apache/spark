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

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import junit.framework.TestCase;
import org.apache.commons.logging.*;

public class TestSequenceFileAsBinaryInputFormat extends TestCase {
  private static final Log LOG = FileInputFormat.LOG;
  private static final int RECORDS = 10000;

  public void testBinary() throws IOException {
    JobConf job = new JobConf();
    FileSystem fs = FileSystem.getLocal(job);
    Path dir = new Path(System.getProperty("test.build.data",".") + "/mapred");
    Path file = new Path(dir, "testbinary.seq");
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);

    fs.delete(dir, true);
    FileInputFormat.setInputPaths(job, dir);

    Text tkey = new Text();
    Text tval = new Text();

    SequenceFile.Writer writer =
      new SequenceFile.Writer(fs, job, file, Text.class, Text.class);
    try {
      for (int i = 0; i < RECORDS; ++i) {
        tkey.set(Integer.toString(r.nextInt(), 36));
        tval.set(Long.toString(r.nextLong(), 36));
        writer.append(tkey, tval);
      }
    } finally {
      writer.close();
    }

    InputFormat<BytesWritable,BytesWritable> bformat =
      new SequenceFileAsBinaryInputFormat();

    int count = 0;
    r.setSeed(seed);
    BytesWritable bkey = new BytesWritable();
    BytesWritable bval = new BytesWritable();
    Text cmpkey = new Text();
    Text cmpval = new Text();
    DataInputBuffer buf = new DataInputBuffer();
    final int NUM_SPLITS = 3;
    FileInputFormat.setInputPaths(job, file);
    for (InputSplit split : bformat.getSplits(job, NUM_SPLITS)) {
      RecordReader<BytesWritable,BytesWritable> reader =
        bformat.getRecordReader(split, job, Reporter.NULL);
      try {
        while (reader.next(bkey, bval)) {
          tkey.set(Integer.toString(r.nextInt(), 36));
          tval.set(Long.toString(r.nextLong(), 36));
          buf.reset(bkey.getBytes(), bkey.getLength());
          cmpkey.readFields(buf);
          buf.reset(bval.getBytes(), bval.getLength());
          cmpval.readFields(buf);
          assertTrue(
              "Keys don't match: " + "*" + cmpkey.toString() + ":" +
                                           tkey.toString() + "*",
              cmpkey.toString().equals(tkey.toString()));
          assertTrue(
              "Vals don't match: " + "*" + cmpval.toString() + ":" +
                                           tval.toString() + "*",
              cmpval.toString().equals(tval.toString()));
          ++count;
        }
      } finally {
        reader.close();
      }
    }
    assertEquals("Some records not found", RECORDS, count);
  }

}
