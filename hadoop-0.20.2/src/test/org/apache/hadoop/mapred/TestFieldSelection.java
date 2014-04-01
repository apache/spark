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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;

import junit.framework.TestCase;
import java.text.NumberFormat;

public class TestFieldSelection extends TestCase {

private static NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setMinimumIntegerDigits(4);
    idFormat.setGroupingUsed(false);
  }

  public void testFieldSelection() throws Exception {
    launch();
  }

  public static void launch() throws Exception {
    JobConf conf = new JobConf(TestFieldSelection.class);
    FileSystem fs = FileSystem.get(conf);
    int numOfInputLines = 10;

    Path OUTPUT_DIR = new Path("build/test/output_for_field_selection_test");
    Path INPUT_DIR = new Path("build/test/input_for_field_selection_test");
    String inputFile = "input.txt";
    fs.delete(INPUT_DIR, true);
    fs.mkdirs(INPUT_DIR);
    fs.delete(OUTPUT_DIR, true);

    StringBuffer inputData = new StringBuffer();
    StringBuffer expectedOutput = new StringBuffer();

    FSDataOutputStream fileOut = fs.create(new Path(INPUT_DIR, inputFile));
    for (int i = 0; i < numOfInputLines; i++) {
        inputData.append(idFormat.format(i));
        inputData.append("-").append(idFormat.format(i+1));
        inputData.append("-").append(idFormat.format(i+2));
        inputData.append("-").append(idFormat.format(i+3));
        inputData.append("-").append(idFormat.format(i+4));
        inputData.append("-").append(idFormat.format(i+5));
        inputData.append("-").append(idFormat.format(i+6));
        inputData.append("\n");


        expectedOutput.append(idFormat.format(i+3));
        expectedOutput.append("-" ).append (idFormat.format(i+2));
        expectedOutput.append("-" ).append (idFormat.format(i+1));
        expectedOutput.append("-" ).append (idFormat.format(i+5));
        expectedOutput.append("-" ).append (idFormat.format(i+6));

        expectedOutput.append("-" ).append (idFormat.format(i+6));
        expectedOutput.append("-" ).append (idFormat.format(i+5));
        expectedOutput.append("-" ).append (idFormat.format(i+1));
        expectedOutput.append("-" ).append (idFormat.format(i+2));
        expectedOutput.append("-" ).append (idFormat.format(i+3));

        expectedOutput.append("-" ).append (idFormat.format(i+0));
        expectedOutput.append("-" ).append (idFormat.format(i+1));
        expectedOutput.append("-" ).append (idFormat.format(i+2));
        expectedOutput.append("-" ).append (idFormat.format(i+3));
        expectedOutput.append("-" ).append (idFormat.format(i+4));
        expectedOutput.append("-" ).append (idFormat.format(i+5));
        expectedOutput.append("-" ).append (idFormat.format(i+6));
        expectedOutput.append("\n");
    }
    fileOut.write(inputData.toString().getBytes("utf-8"));
    fileOut.close();

    System.out.println("inputData:");
    System.out.println(inputData.toString());
    JobConf job = new JobConf(conf, TestFieldSelection.class);
    FileInputFormat.setInputPaths(job, INPUT_DIR);
    job.setInputFormat(TextInputFormat.class);
    job.setMapperClass(FieldSelectionMapReduce.class);
    job.setReducerClass(FieldSelectionMapReduce.class);

    FileOutputFormat.setOutputPath(job, OUTPUT_DIR);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setNumReduceTasks(1);

    job.set("mapred.data.field.separator", "-");
    job.set("map.output.key.value.fields.spec", "6,5,1-3:0-");
    job.set("reduce.output.key.value.fields.spec", ":4,3,2,1,0,0-");

    JobClient.runJob(job);

    //
    // Finally, we compare the reconstructed answer key with the
    // original one.  Remember, we need to ignore zero-count items
    // in the original key.
    //
    boolean success = true;
    Path outPath = new Path(OUTPUT_DIR, "part-00000");
    String outdata = MapReduceTestUtil.readOutput(outPath,job);

    assertEquals(expectedOutput.toString(),outdata);
    fs.delete(OUTPUT_DIR, true);
    fs.delete(INPUT_DIR, true);
  }

  /**
   * Launches all the tasks in order.
   */
  public static void main(String[] argv) throws Exception {
    launch();
  }
}
