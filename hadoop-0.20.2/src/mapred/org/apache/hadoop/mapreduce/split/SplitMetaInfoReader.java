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

package org.apache.hadoop.mapreduce.split;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;

/**
 * A (internal) utility that reads the split meta info and creates
 * split meta info objects
 */

public class SplitMetaInfoReader {
  
  public static JobSplit.TaskSplitMetaInfo[] readSplitMetaInfo(
      JobID jobId, FileSystem fs, Configuration conf, Path jobSubmitDir) 
  throws IOException {
    long maxMetaInfoSize = conf.getLong("mapreduce.jobtracker.split.metainfo.maxsize", 
        10000000L);
    Path metaSplitFile = JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir);
    FileStatus fStatus = fs.getFileStatus(metaSplitFile);
    if (maxMetaInfoSize > 0 && fStatus.getLen() > maxMetaInfoSize) {
      throw new IOException("Split metadata size exceeded " +
          maxMetaInfoSize +". Aborting job " + jobId);
    }
    FSDataInputStream in = fs.open(metaSplitFile);
    byte[] header = new byte[JobSplit.META_SPLIT_FILE_HEADER.length];
    in.readFully(header);
    if (!Arrays.equals(JobSplit.META_SPLIT_FILE_HEADER, header)) {
      throw new IOException("Invalid header on split file");
    }
    int vers = WritableUtils.readVInt(in);
    if (vers != JobSplit.META_SPLIT_VERSION) {
      in.close();
      throw new IOException("Unsupported split version " + vers);
    }
    int numSplits = WritableUtils.readVInt(in); //TODO: check for insane values
    JobSplit.TaskSplitMetaInfo[] allSplitMetaInfo = 
      new JobSplit.TaskSplitMetaInfo[numSplits];
    final int maxLocations =
      conf.getInt(JobSplitWriter.MAX_SPLIT_LOCATIONS, Integer.MAX_VALUE);
    for (int i = 0; i < numSplits; i++) {
      JobSplit.SplitMetaInfo splitMetaInfo = new JobSplit.SplitMetaInfo();
      splitMetaInfo.readFields(in);
      final int numLocations = splitMetaInfo.getLocations().length;
      if (numLocations > maxLocations) {
        throw new IOException("Max block location exceeded for split: #"  + i +
              " splitsize: " + numLocations + " maxsize: " + maxLocations);
      }
      JobSplit.TaskSplitIndex splitIndex = new JobSplit.TaskSplitIndex(
          JobSubmissionFiles.getJobSplitFile(jobSubmitDir).toString(), 
          splitMetaInfo.getStartOffset());
      allSplitMetaInfo[i] = new JobSplit.TaskSplitMetaInfo(splitIndex, 
          splitMetaInfo.getLocations(), 
          splitMetaInfo.getInputDataLength());
    }
    in.close();
    return allSplitMetaInfo;
  }

}
