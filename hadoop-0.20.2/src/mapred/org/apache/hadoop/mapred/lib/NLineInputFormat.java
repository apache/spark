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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.LineReader;

/**
 * NLineInputFormat which splits N lines of input as one split.
 *
 * In many "pleasantly" parallel applications, each process/mapper 
 * processes the same input file (s), but with computations are 
 * controlled by different parameters.(Referred to as "parameter sweeps").
 * One way to achieve this, is to specify a set of parameters 
 * (one set per line) as input in a control file 
 * (which is the input path to the map-reduce application,
 * where as the input dataset is specified 
 * via a config variable in JobConf.).
 * 
 * The NLineInputFormat can be used in such applications, that splits 
 * the input file such that by default, one line is fed as
 * a value to one map task, and key is the offset.
 * i.e. (k,v) is (LongWritable, Text).
 * The location hints will span the whole mapred cluster.
 */

public class NLineInputFormat extends FileInputFormat<LongWritable, Text> 
                              implements JobConfigurable { 
  private int N = 1;

  public RecordReader<LongWritable, Text> getRecordReader(
                                            InputSplit genericSplit,
                                            JobConf job,
                                            Reporter reporter) 
  throws IOException {
    reporter.setStatus(genericSplit.toString());
    return new LineRecordReader(job, (FileSplit) genericSplit);
  }

  /** 
   * Logically splits the set of input files for the job, splits N lines
   * of the input as one split.
   * 
   * @see org.apache.hadoop.mapred.FileInputFormat#getSplits(JobConf, int)
   */
  public InputSplit[] getSplits(JobConf job, int numSplits)
  throws IOException {
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>();
    for (FileStatus status : listStatus(job)) {
      Path fileName = status.getPath();
      if (status.isDir()) {
        throw new IOException("Not a file: " + fileName);
      }
      FileSystem  fs = fileName.getFileSystem(job);
      LineReader lr = null;
      try {
        FSDataInputStream in  = fs.open(fileName);
        lr = new LineReader(in, job);
        Text line = new Text();
        int numLines = 0;
        long begin = 0;
        long length = 0;
        int num = -1;
        while ((num = lr.readLine(line)) > 0) {
          numLines++;
          length += num;
          if (numLines == N) {
            splits.add(new FileSplit(fileName, begin, length, new String[]{}));
            begin += length;
            length = 0;
            numLines = 0;
          }
        }
        if (numLines != 0) {
          splits.add(new FileSplit(fileName, begin, length, new String[]{}));
        }
   
      } finally {
        if (lr != null) {
          lr.close();
        }
      }
    }
    return splits.toArray(new FileSplit[splits.size()]);
  }

  public void configure(JobConf conf) {
    N = conf.getInt("mapred.line.input.format.linespermap", 1);
  }
}
