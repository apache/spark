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

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import junit.framework.TestCase;
import org.apache.commons.logging.*;

public class TestMRSequenceFileAsBinaryOutputFormat extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestMRSequenceFileAsBinaryOutputFormat.class.getName());

  private static final int RECORDS = 10000;
  
  public void testBinary() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    Job job = new Job(conf);
    
    Path outdir = new Path(System.getProperty("test.build.data", "/tmp"),
                    "outseq");
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);

    FileOutputFormat.setOutputPath(job, outdir);

    SequenceFileAsBinaryOutputFormat.setSequenceFileOutputKeyClass(job, 
                                          IntWritable.class );
    SequenceFileAsBinaryOutputFormat.setSequenceFileOutputValueClass(job, 
                                          DoubleWritable.class ); 

    SequenceFileAsBinaryOutputFormat.setCompressOutput(job, true);
    SequenceFileAsBinaryOutputFormat.setOutputCompressionType(job, 
                                                       CompressionType.BLOCK);

    BytesWritable bkey = new BytesWritable();
    BytesWritable bval = new BytesWritable();

    TaskAttemptContext context = 
      MapReduceTestUtil.createDummyMapTaskAttemptContext(job.getConfiguration());
    OutputFormat<BytesWritable, BytesWritable> outputFormat = 
      new SequenceFileAsBinaryOutputFormat();
    OutputCommitter committer = outputFormat.getOutputCommitter(context);
    committer.setupJob(job);
    RecordWriter<BytesWritable, BytesWritable> writer = outputFormat.
      getRecordWriter(context);

    IntWritable iwritable = new IntWritable();
    DoubleWritable dwritable = new DoubleWritable();
    DataOutputBuffer outbuf = new DataOutputBuffer();
    LOG.info("Creating data by SequenceFileAsBinaryOutputFormat");
    try {
      for (int i = 0; i < RECORDS; ++i) {
        iwritable = new IntWritable(r.nextInt());
        iwritable.write(outbuf);
        bkey.set(outbuf.getData(), 0, outbuf.getLength());
        outbuf.reset();
        dwritable = new DoubleWritable(r.nextDouble());
        dwritable.write(outbuf);
        bval.set(outbuf.getData(), 0, outbuf.getLength());
        outbuf.reset();
        writer.write(bkey, bval);
      }
    } finally {
      writer.close(context);
    }
    committer.commitTask(context);
    committer.commitJob(job);

    InputFormat<IntWritable, DoubleWritable> iformat =
      new SequenceFileInputFormat<IntWritable, DoubleWritable>();
    int count = 0;
    r.setSeed(seed);
    SequenceFileInputFormat.setInputPaths(job, outdir);
    LOG.info("Reading data by SequenceFileInputFormat");
    for (InputSplit split : iformat.getSplits(job)) {
      RecordReader<IntWritable, DoubleWritable> reader =
        iformat.createRecordReader(split, context);
      MapContext<IntWritable, DoubleWritable, BytesWritable, BytesWritable> 
        mcontext = new MapContext<IntWritable, DoubleWritable,
          BytesWritable, BytesWritable>(job.getConfiguration(), 
          context.getTaskAttemptID(), reader, null, null, 
          MapReduceTestUtil.createDummyReporter(), 
          split);
      reader.initialize(split, mcontext);
      try {
        int sourceInt;
        double sourceDouble;
        while (reader.nextKeyValue()) {
          sourceInt = r.nextInt();
          sourceDouble = r.nextDouble();
          iwritable = reader.getCurrentKey();
          dwritable = reader.getCurrentValue();
          assertEquals(
              "Keys don't match: " + "*" + iwritable.get() + ":" + 
                                           sourceInt + "*",
              sourceInt, iwritable.get());
          assertTrue(
              "Vals don't match: " + "*" + dwritable.get() + ":" +
                                           sourceDouble + "*",
              Double.compare(dwritable.get(), sourceDouble) == 0 );
          ++count;
        }
      } finally {
        reader.close();
      }
    }
    assertEquals("Some records not found", RECORDS, count);
  }

  public void testSequenceOutputClassDefaultsToMapRedOutputClass() 
         throws IOException {
    Job job = new Job();
    // Setting Random class to test getSequenceFileOutput{Key,Value}Class
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(BooleanWritable.class);

    assertEquals("SequenceFileOutputKeyClass should default to ouputKeyClass", 
      FloatWritable.class,
      SequenceFileAsBinaryOutputFormat.getSequenceFileOutputKeyClass(job));
    assertEquals("SequenceFileOutputValueClass should default to " 
      + "ouputValueClass", 
      BooleanWritable.class,
      SequenceFileAsBinaryOutputFormat.getSequenceFileOutputValueClass(job));

    SequenceFileAsBinaryOutputFormat.setSequenceFileOutputKeyClass(job, 
      IntWritable.class );
    SequenceFileAsBinaryOutputFormat.setSequenceFileOutputValueClass(job, 
      DoubleWritable.class ); 

    assertEquals("SequenceFileOutputKeyClass not updated", 
      IntWritable.class,
      SequenceFileAsBinaryOutputFormat.getSequenceFileOutputKeyClass(job));
    assertEquals("SequenceFileOutputValueClass not updated", 
      DoubleWritable.class,
      SequenceFileAsBinaryOutputFormat.getSequenceFileOutputValueClass(job));
  }

  public void testcheckOutputSpecsForbidRecordCompression() 
      throws IOException {
    Job job = new Job();
    FileSystem fs = FileSystem.getLocal(job.getConfiguration());
    Path outputdir = new Path(System.getProperty("test.build.data", "/tmp") 
                              + "/output");
    fs.delete(outputdir, true);

    // Without outputpath, FileOutputFormat.checkoutputspecs will throw 
    // InvalidJobConfException
    FileOutputFormat.setOutputPath(job, outputdir);

    // SequenceFileAsBinaryOutputFormat doesn't support record compression
    // It should throw an exception when checked by checkOutputSpecs
    SequenceFileAsBinaryOutputFormat.setCompressOutput(job, true);

    SequenceFileAsBinaryOutputFormat.setOutputCompressionType(job, 
      CompressionType.BLOCK);
    try {
      new SequenceFileAsBinaryOutputFormat().checkOutputSpecs(job);
    } catch (Exception e) {
      fail("Block compression should be allowed for " 
        + "SequenceFileAsBinaryOutputFormat:Caught " + e.getClass().getName());
    }

    SequenceFileAsBinaryOutputFormat.setOutputCompressionType(job, 
      CompressionType.RECORD);
    try {
      new SequenceFileAsBinaryOutputFormat().checkOutputSpecs(job);
      fail("Record compression should not be allowed for " 
        + "SequenceFileAsBinaryOutputFormat");
    } catch (InvalidJobConfException ie) {
      // expected
    } catch (Exception e) {
      fail("Expected " + InvalidJobConfException.class.getName() 
        + "but caught " + e.getClass().getName() );
    }
  }
}
