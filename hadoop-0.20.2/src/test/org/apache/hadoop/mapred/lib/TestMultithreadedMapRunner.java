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

package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class TestMultithreadedMapRunner extends HadoopTestCase {

  public TestMultithreadedMapRunner() throws IOException {
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
    Path inDir = new Path("testing/mt/input");
    Path outDir = new Path("testing/mt/output");

    // Hack for local FS that does not have the concept of a 'mounting point'
    if (isLocalFS()) {
      String localPathRoot = System.getProperty("test.build.data", "/tmp")
              .replace(' ', '+');
      inDir = new Path(localPathRoot, inDir);
      outDir = new Path(localPathRoot, outDir);
    }


    JobConf conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);

    fs.delete(outDir, true);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = fs.create(new Path(inDir, "part-0"));
      file.writeBytes("a\nb\n\nc\nd\ne");
      file.close();
    }

    conf.setJobName("mt");
    conf.setInputFormat(TextInputFormat.class);

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(IDMap.class);
    conf.setReducerClass(IDReduce.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);

    conf.setMapRunnerClass(MultithreadedMapRunner.class);
    
    conf.setInt("mapred.map.multithreadedrunner.threads", 2);

    if (ioEx) {
      conf.setBoolean("multithreaded.ioException", true);
    }
    if (rtEx) {
      conf.setBoolean("multithreaded.runtimeException", true);
    }
    JobClient jc = new JobClient(conf);
    RunningJob job =jc.submitJob(conf);
    while (!job.isComplete()) {
      Thread.sleep(100);
    }

    if (job.isSuccessful()) {
      assertFalse(ioEx || rtEx);
    }
    else {
      assertTrue(ioEx || rtEx);
    }

  }

  public static class IDMap implements Mapper<LongWritable, Text,
                                              LongWritable, Text> {
    private boolean ioEx = false;
    private boolean rtEx = false;

    public void configure(JobConf job) {
      ioEx = job.getBoolean("multithreaded.ioException", false);
      rtEx = job.getBoolean("multithreaded.runtimeException", false);
    }

    public void map(LongWritable key, Text value,
                    OutputCollector<LongWritable, Text> output,
                    Reporter reporter)
            throws IOException {
      if (ioEx) {
        throw new IOException();
      }
      if (rtEx) {
        throw new RuntimeException();
      }
      output.collect(key, value);
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }


    public void close() throws IOException {
    }
  }

  public static class IDReduce implements Reducer<LongWritable, Text,
                                                  LongWritable, Text> {

    public void configure(JobConf job) {
    }

    public void reduce(LongWritable key, Iterator<Text> values,
                       OutputCollector<LongWritable, Text> output,
                       Reporter reporter)
            throws IOException {
      while (values.hasNext()) {
        output.collect(key, values.next());
      }
    }

    public void close() throws IOException {
    }
  }
}

