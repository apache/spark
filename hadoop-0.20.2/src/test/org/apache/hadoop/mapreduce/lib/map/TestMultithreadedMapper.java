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

package org.apache.hadoop.mapreduce.lib.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class TestMultithreadedMapper extends HadoopTestCase {

  public TestMultithreadedMapper() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  public void testOKRun() throws Exception {
    run(false, false);
  }

  public void testIOExRun() throws Exception {
    run(true, false);
  }
  public void testRuntimeExRun() throws Exception {
    run(false, true);
  }

  private void run(boolean ioEx, boolean rtEx) throws Exception {
    String localPathRoot = System.getProperty("test.build.data", "/tmp");
    Path inDir = new Path(localPathRoot, "testing/mt/input");
    Path outDir = new Path(localPathRoot, "testing/mt/output");


    Configuration conf = createJobConf();
    if (ioEx) {
      conf.setBoolean("multithreaded.ioException", true);
    }
    if (rtEx) {
      conf.setBoolean("multithreaded.runtimeException", true);
    }

    Job job = new Job(conf);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    if (fs.exists(inDir)) {
      fs.delete(inDir, true);
    }
    fs.mkdirs(inDir);
    String input = "The quick brown fox\n" + "has many silly\n"
      + "red fox sox\n";
    DataOutputStream file = fs.create(new Path(inDir, "part-" + 0));
    file.writeBytes(input);
    file.close();

    FileInputFormat.setInputPaths(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir);
    job.setNumReduceTasks(1);
    job.setJobName("mt");

    job.setMapperClass(MultithreadedMapper.class);
    MultithreadedMapper.setMapperClass(job, IDMap.class);
    MultithreadedMapper.setNumberOfThreads(job, 2);
    job.setReducerClass(Reducer.class);

    job.waitForCompletion(true);

    if (job.isSuccessful()) {
      assertFalse(ioEx || rtEx);
    }
    else {
      assertTrue(ioEx || rtEx);
    }
  }

  public static class IDMap extends 
      Mapper<LongWritable, Text, LongWritable, Text> {
    private boolean ioEx = false;
    private boolean rtEx = false;

    public void setup(Context context) {
      ioEx = context.getConfiguration().
               getBoolean("multithreaded.ioException", false);
      rtEx = context.getConfiguration().
               getBoolean("multithreaded.runtimeException", false);
    }

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      if (ioEx) {
        throw new IOException();
      }
      if (rtEx) {
        throw new RuntimeException();
      }
      super.map(key, value, context);
    }
  }
}
