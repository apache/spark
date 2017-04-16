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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

public class TestFileOutputFormat extends HadoopTestCase {

  public TestFileOutputFormat() throws IOException {
    super(HadoopTestCase.CLUSTER_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  public void testCustomFile() throws Exception {
    Path inDir = new Path("testing/fileoutputformat/input");
    Path outDir = new Path("testing/fileoutputformat/output");

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

    DataOutputStream file = fs.create(new Path(inDir, "part-0"));
    file.writeBytes("a\nb\n\nc\nd\ne");
    file.close();

    file = fs.create(new Path(inDir, "part-1"));
    file.writeBytes("a\nb\n\nc\nd\ne");
    file.close();

    conf.setJobName("fof");
    conf.setInputFormat(TextInputFormat.class);

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(TestMap.class);
    conf.setReducerClass(TestReduce.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);

    JobClient jc = new JobClient(conf);
    RunningJob job = jc.submitJob(conf);
    while (!job.isComplete()) {
      Thread.sleep(100);
    }
    assertTrue(job.isSuccessful());

    boolean map0 = false;
    boolean map1 = false;
    boolean reduce = false;
    FileStatus[] statuses = fs.listStatus(outDir);
    for (FileStatus status : statuses) {
      map0 = map0 || status.getPath().getName().equals("test-m-00000");
      map1 = map1 || status.getPath().getName().equals("test-m-00001");
      reduce = reduce || status.getPath().getName().equals("test-r-00000");
    }

    assertTrue(map0);
    assertTrue(map1);
    assertTrue(reduce);
  }

  public static class TestMap implements Mapper<LongWritable, Text,
    LongWritable, Text> {

    public void configure(JobConf conf) {
      try {
        FileSystem fs = FileSystem.get(conf);
        OutputStream os =
          fs.create(FileOutputFormat.getPathForCustomFile(conf, "test"));
        os.write(1);
        os.close();
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    public void map(LongWritable key, Text value,
                    OutputCollector<LongWritable, Text> output,
                    Reporter reporter) throws IOException {
      output.collect(key, value);
    }

    public void close() throws IOException {
    }
  }

  public static class TestReduce implements Reducer<LongWritable, Text,
    LongWritable, Text> {

    public void configure(JobConf conf) {
      try {
        FileSystem fs = FileSystem.get(conf);
        OutputStream os =
          fs.create(FileOutputFormat.getPathForCustomFile(conf, "test"));
        os.write(1);
        os.close();
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    public void reduce(LongWritable key, Iterator<Text> values,
                       OutputCollector<LongWritable, Text> output,
                       Reporter reporter) throws IOException {
      while (values.hasNext()) {
        Text value = values.next();
        output.collect(key, value);
      }
    }

    public void close() throws IOException {
    }
  }

}
