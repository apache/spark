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
package org.apache.hadoop.mapred.lib.aggregate;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;

import junit.framework.TestCase;
import java.io.*;
import java.util.*;
import java.text.NumberFormat;

public class TestAggregates extends TestCase {

  private static NumberFormat idFormat = NumberFormat.getInstance();
    static {
      idFormat.setMinimumIntegerDigits(4);
      idFormat.setGroupingUsed(false);
  }


  public void testAggregates() throws Exception {
    launch();
  }

  public static void launch() throws Exception {
    JobConf conf = new JobConf(TestAggregates.class);
    FileSystem fs = FileSystem.get(conf);
    int numOfInputLines = 20;

    Path OUTPUT_DIR = new Path("build/test/output_for_aggregates_test");
    Path INPUT_DIR = new Path("build/test/input_for_aggregates_test");
    String inputFile = "input.txt";
    fs.delete(INPUT_DIR, true);
    fs.mkdirs(INPUT_DIR);
    fs.delete(OUTPUT_DIR, true);

    StringBuffer inputData = new StringBuffer();
    StringBuffer expectedOutput = new StringBuffer();
    expectedOutput.append("max\t19\n");
    expectedOutput.append("min\t1\n"); 

    FSDataOutputStream fileOut = fs.create(new Path(INPUT_DIR, inputFile));
    for (int i = 1; i < numOfInputLines; i++) {
      expectedOutput.append("count_").append(idFormat.format(i));
      expectedOutput.append("\t").append(i).append("\n");

      inputData.append(idFormat.format(i));
      for (int j = 1; j < i; j++) {
        inputData.append(" ").append(idFormat.format(i));
      }
      inputData.append("\n");
    }
    expectedOutput.append("value_as_string_max\t9\n");
    expectedOutput.append("value_as_string_min\t1\n");
    expectedOutput.append("uniq_count\t15\n");


    fileOut.write(inputData.toString().getBytes("utf-8"));
    fileOut.close();

    System.out.println("inputData:");
    System.out.println(inputData.toString());
    JobConf job = new JobConf(conf, TestAggregates.class);
    FileInputFormat.setInputPaths(job, INPUT_DIR);
    job.setInputFormat(TextInputFormat.class);

    FileOutputFormat.setOutputPath(job, OUTPUT_DIR);
    job.setOutputFormat(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);

    job.setMapperClass(ValueAggregatorMapper.class);
    job.setReducerClass(ValueAggregatorReducer.class);
    job.setCombinerClass(ValueAggregatorCombiner.class);

    job.setInt("aggregator.descriptor.num", 1);
    job.set("aggregator.descriptor.0", 
          "UserDefined,org.apache.hadoop.mapred.lib.aggregate.AggregatorTests");
    job.setLong("aggregate.max.num.unique.values", 14);

    JobClient.runJob(job);

    //
    // Finally, we compare the reconstructed answer key with the
    // original one.  Remember, we need to ignore zero-count items
    // in the original key.
    //
    boolean success = true;
    Path outPath = new Path(OUTPUT_DIR, "part-00000");
    String outdata = MapReduceTestUtil.readOutput(outPath,job);
    System.out.println("full out data:");
    System.out.println(outdata.toString());
    outdata = outdata.substring(0, expectedOutput.toString().length());

    assertEquals(expectedOutput.toString(),outdata);
    //fs.delete(OUTPUT_DIR);
    fs.delete(INPUT_DIR, true);
  }

  /**
   * Launches all the tasks in order.
   */
  public static void main(String[] argv) throws Exception {
    launch();
  }
}
