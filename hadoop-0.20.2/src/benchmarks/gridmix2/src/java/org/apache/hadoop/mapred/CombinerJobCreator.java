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
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class CombinerJobCreator {

   public static class MapClass extends MapReduceBase
     implements Mapper<LongWritable, Text, Text, IntWritable> {

     private final static IntWritable one = new IntWritable(1);
     private Text word = new Text();

     public void map(LongWritable key, Text value,
                     OutputCollector<Text, IntWritable> output,
                     Reporter reporter) throws IOException {
       String line = value.toString();
       StringTokenizer itr = new StringTokenizer(line);
       while (itr.hasMoreTokens()) {
         word.set(itr.nextToken());
         output.collect(word, one);
       }
     }
   }

   public static class Reduce extends MapReduceBase
     implements Reducer<Text, IntWritable, Text, IntWritable> {

     public void reduce(Text key, Iterator<IntWritable> values,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
       int sum = 0;
       while (values.hasNext()) {
         sum += values.next().get();
       }
       output.collect(key, new IntWritable(sum));
     }
   }

  public static JobConf createJob(String[] args) throws Exception {
    JobConf conf = new JobConf(CombinerJobCreator.class);
    conf.setJobName("GridmixCombinerJob");

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(MapClass.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    boolean mapoutputCompressed = false;
    boolean outputCompressed = false;
    // List<String> other_args = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else if ("-indir".equals(args[i])) {
          FileInputFormat.setInputPaths(conf, args[++i]);
        } else if ("-outdir".equals(args[i])) {
          FileOutputFormat.setOutputPath(conf, new Path(args[++i]));

        } else if ("-mapoutputCompressed".equals(args[i])) {
          mapoutputCompressed = Boolean.valueOf(args[++i]).booleanValue();
        } else if ("-outputCompressed".equals(args[i])) {
          outputCompressed = Boolean.valueOf(args[++i]).booleanValue();
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return null;
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from "
            + args[i - 1]);
        return null;
      }
    }
    conf.setCompressMapOutput(mapoutputCompressed);
    conf.setBoolean("mapred.output.compress", outputCompressed);
    return conf;
  }
}
