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

import java.util.Random;
import java.util.Stack;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.GenericMRLoadGenerator;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapred.JobConf;

public class GenericMRLoadJobCreator extends GenericMRLoadGenerator {

  public static JobConf createJob(String[] argv, boolean mapoutputCompressed,
      boolean outputCompressed) throws Exception {

    JobConf job = new JobConf();
    job.setJarByClass(GenericMRLoadGenerator.class);
    job.setMapperClass(SampleMapper.class);
    job.setReducerClass(SampleReducer.class);
    if (!parseArgs(argv, job)) {
      return null;
    }

    if (null == FileOutputFormat.getOutputPath(job)) {
      // No output dir? No writes
      job.setOutputFormat(NullOutputFormat.class);
    }

    if (0 == FileInputFormat.getInputPaths(job).length) {
      // No input dir? Generate random data
      System.err.println("No input path; ignoring InputFormat");
      confRandom(job);
    } else if (null != job.getClass("mapred.indirect.input.format", null)) {
      // specified IndirectInputFormat? Build src list
      JobClient jClient = new JobClient(job);
      Path sysdir = jClient.getSystemDir();
      Random r = new Random();
      Path indirInputFile = new Path(sysdir, Integer.toString(r
          .nextInt(Integer.MAX_VALUE), 36)
          + "_files");
      job.set("mapred.indirect.input.file", indirInputFile.toString());
      SequenceFile.Writer writer = SequenceFile.createWriter(sysdir
          .getFileSystem(job), job, indirInputFile, LongWritable.class,
          Text.class, SequenceFile.CompressionType.NONE);
      try {
        for (Path p : FileInputFormat.getInputPaths(job)) {
          FileSystem fs = p.getFileSystem(job);
          Stack<Path> pathstack = new Stack<Path>();
          pathstack.push(p);
          while (!pathstack.empty()) {
            for (FileStatus stat : fs.listStatus(pathstack.pop())) {
              if (stat.isDir()) {
                if (!stat.getPath().getName().startsWith("_")) {
                  pathstack.push(stat.getPath());
                }
              } else {
                writer.sync();
                writer.append(new LongWritable(stat.getLen()), new Text(stat
                    .getPath().toUri().toString()));
              }
            }
          }
        }
      } finally {
        writer.close();
      }
    }

    job.setCompressMapOutput(mapoutputCompressed);
    job.setBoolean("mapred.output.compress", outputCompressed);
    return job;

  }

}
