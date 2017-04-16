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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

/**
 * @see TestDelegatingInputFormat
 */
public class TestMultipleInputs extends HadoopTestCase {

  public TestMultipleInputs() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  private static final Path ROOT_DIR = new Path("testing/mo");
  private static final Path IN1_DIR = new Path(ROOT_DIR, "input1");
  private static final Path IN2_DIR = new Path(ROOT_DIR, "input2");
  private static final Path OUT_DIR = new Path(ROOT_DIR, "output");

  private Path getDir(Path dir) {
    // Hack for local FS that does not have the concept of a 'mounting point'
    if (isLocalFS()) {
      String localPathRoot = System.getProperty("test.build.data", "/tmp")
          .replace(' ', '+');
      dir = new Path(localPathRoot, dir);
    }
    return dir;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Path rootDir = getDir(ROOT_DIR);
    Path in1Dir = getDir(IN1_DIR);
    Path in2Dir = getDir(IN2_DIR);

    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    fs.delete(rootDir, true);
    if (!fs.mkdirs(in1Dir)) {
      throw new IOException("Mkdirs failed to create " + in1Dir.toString());
    }
    if (!fs.mkdirs(in2Dir)) {
      throw new IOException("Mkdirs failed to create " + in2Dir.toString());
    }
  }

  @Test
  public void testDoMultipleInputs() throws IOException {
    Path in1Dir = getDir(IN1_DIR);
    Path in2Dir = getDir(IN2_DIR);

    Path outDir = getDir(OUT_DIR);

    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    fs.delete(outDir, true);

    DataOutputStream file1 = fs.create(new Path(in1Dir, "part-0"));
    file1.writeBytes("a\nb\nc\nd\ne");
    file1.close();

    // write tab delimited to second file because we're doing
    // KeyValueInputFormat
    DataOutputStream file2 = fs.create(new Path(in2Dir, "part-0"));
    file2.writeBytes("a\tblah\nb\tblah\nc\tblah\nd\tblah\ne\tblah");
    file2.close();

    Job job = new Job(conf);
    job.setJobName("mi");

    MultipleInputs.addInputPath(job, in1Dir, TextInputFormat.class,
        MapClass.class);
    MultipleInputs.addInputPath(job, in2Dir, KeyValueTextInputFormat.class,
        KeyValueMapClass.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setReducerClass(ReducerClass.class);
    FileOutputFormat.setOutputPath(job, outDir);

    boolean success = false;
    try {
      success = job.waitForCompletion(true);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    } catch (ClassNotFoundException instante) {
      throw new RuntimeException(instante);
    }
    if (!success)
      throw new RuntimeException("Job failed!");

    // copy bytes a bunch of times for the ease of readLine() - whatever
    BufferedReader output = new BufferedReader(new InputStreamReader(fs
        .open(new Path(outDir, "part-r-00000"))));
    // reducer should have counted one key from each file
    assertTrue(output.readLine().equals("a 2"));
    assertTrue(output.readLine().equals("b 2"));
    assertTrue(output.readLine().equals("c 2"));
    assertTrue(output.readLine().equals("d 2"));
    assertTrue(output.readLine().equals("e 2"));
  }

  @SuppressWarnings("unchecked")
  public void testAddInputPathWithFormat() throws IOException {
    final Job conf = new Job();
    MultipleInputs.addInputPath(conf, new Path("/foo"), TextInputFormat.class);
    MultipleInputs.addInputPath(conf, new Path("/bar"),
        KeyValueTextInputFormat.class);
    final Map<Path, InputFormat> inputs = MultipleInputs
       .getInputFormatMap(conf);
    assertEquals(TextInputFormat.class, inputs.get(new Path("/foo")).getClass());
    assertEquals(KeyValueTextInputFormat.class, inputs.get(new Path("/bar"))
       .getClass());
  }

  @SuppressWarnings("unchecked")
  public void testAddInputPathWithMapper() throws IOException {
    final Job conf = new Job();
    MultipleInputs.addInputPath(conf, new Path("/foo"), TextInputFormat.class,
       MapClass.class);
    MultipleInputs.addInputPath(conf, new Path("/bar"),
        KeyValueTextInputFormat.class, KeyValueMapClass.class);
    final Map<Path, InputFormat> inputs = MultipleInputs
       .getInputFormatMap(conf);
    final Map<Path, Class<? extends Mapper>> maps = MultipleInputs
       .getMapperTypeMap(conf);

    assertEquals(TextInputFormat.class, inputs.get(new Path("/foo")).getClass());
    assertEquals(KeyValueTextInputFormat.class, inputs.get(new Path("/bar"))
       .getClass());
    assertEquals(MapClass.class, maps.get(new Path("/foo")));
    assertEquals(KeyValueMapClass.class, maps.get(new Path("/bar")));
  }

  static final Text blah = new Text("blah");

  // these 3 classes do a reduce side join with 2 different mappers
  static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
    // receives "a", "b", "c" as values
    @Override
    public void map(LongWritable key, Text value, Context ctx)
        throws IOException, InterruptedException {
      ctx.write(value, blah);
    }
  }

  static class KeyValueMapClass extends Mapper<Text, Text, Text, Text> {
    // receives "a", "b", "c" as keys
    @Override
    public void map(Text key, Text value, Context ctx) throws IOException,
        InterruptedException {
      ctx.write(key, blah);
    }
  }

  static class ReducerClass extends Reducer<Text, Text, NullWritable, Text> {
    // should receive 2 rows for each key
    int count = 0;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context ctx)
        throws IOException, InterruptedException {
      count = 0;
      for (Text value : values)
        count++;
      ctx.write(NullWritable.get(), new Text(key.toString() + " " + count));
    }
  }

}
