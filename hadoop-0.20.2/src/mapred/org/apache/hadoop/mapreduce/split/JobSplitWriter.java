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
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.split.JobSplit.SplitMetaInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The class that is used by the Job clients to write splits (both the meta
 * and the raw bytes parts)
 */
public class JobSplitWriter {

  private static final Log LOG = LogFactory.getLog(JobSplitWriter.class);
  private static final int splitVersion = JobSplit.META_SPLIT_VERSION;
  private static final byte[] SPLIT_FILE_HEADER;
  static final String MAX_SPLIT_LOCATIONS = "mapreduce.job.max.split.locations";
  
  static {
    try {
      SPLIT_FILE_HEADER = "SPL".getBytes("UTF-8");
    } catch (UnsupportedEncodingException u) {
      throw new RuntimeException(u);
    }
  }
  
  @SuppressWarnings("unchecked")
  public static <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, List<InputSplit> splits) 
  throws IOException, InterruptedException {
    T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);
    createSplitFiles(jobSubmitDir, conf, fs, array);
  }
  
  public static <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, T[] splits) 
  throws IOException, InterruptedException {
    FSDataOutputStream out = createFile(fs, 
        JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf);
    SplitMetaInfo[] info = writeNewSplits(conf, splits, out);
    out.close();
    writeJobSplitMetaInfo(fs,JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir), 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion,
        info);
  }
  
  public static void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem   fs, 
      org.apache.hadoop.mapred.InputSplit[] splits) 
  throws IOException {
    FSDataOutputStream out = createFile(fs, 
        JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf);
    SplitMetaInfo[] info = writeOldSplits(splits, out, conf);
    out.close();
    writeJobSplitMetaInfo(fs,JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir), 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion,
        info);
  }
  
  private static FSDataOutputStream createFile(FileSystem fs, Path splitFile, 
      Configuration job)  throws IOException {
    FSDataOutputStream out = FileSystem.create(fs, splitFile, 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
    int replication = job.getInt("mapred.submit.replication", 10);
    fs.setReplication(splitFile, (short)replication);
    writeSplitHeader(out);
    return out;
  }
  private static void writeSplitHeader(FSDataOutputStream out) 
  throws IOException {
    out.write(SPLIT_FILE_HEADER);
    out.writeInt(splitVersion);
  }
  
  @SuppressWarnings("unchecked")
  private static <T extends InputSplit> 
  SplitMetaInfo[] writeNewSplits(Configuration conf, 
      T[] array, FSDataOutputStream out)
  throws IOException, InterruptedException {

    SplitMetaInfo[] info = new SplitMetaInfo[array.length];
    if (array.length != 0) {
      SerializationFactory factory = new SerializationFactory(conf);
      int i = 0;
      long offset = out.size();
      for(T split: array) {
        int prevCount = out.size();
        Text.writeString(out, split.getClass().getName());
        Serializer<T> serializer = 
          factory.getSerializer((Class<T>) split.getClass());
        serializer.open(out);
        serializer.serialize(split);
        int currCount = out.size();
        String[] locations = split.getLocations();
        final int max_loc = conf.getInt(MAX_SPLIT_LOCATIONS, 10);
        if (locations.length > max_loc) {
          LOG.warn("Max block location exceeded for split: "
              + split + " splitsize: " + locations.length +
              " maxsize: " + max_loc);
          locations = Arrays.copyOf(locations, max_loc);
        }
        info[i++] = 
          new JobSplit.SplitMetaInfo( 
              locations, offset,
              split.getLength());
        offset += currCount - prevCount;
      }
    }
    return info;
  }
  
  private static SplitMetaInfo[] writeOldSplits(
      org.apache.hadoop.mapred.InputSplit[] splits,
      FSDataOutputStream out, Configuration conf) throws IOException {
    SplitMetaInfo[] info = new SplitMetaInfo[splits.length];
    if (splits.length != 0) {
      int i = 0;
      long offset = out.size();
      for(org.apache.hadoop.mapred.InputSplit split: splits) {
        int prevLen = out.size();
        Text.writeString(out, split.getClass().getName());
        split.write(out);
        int currLen = out.size();
        String[] locations = split.getLocations();
        final int max_loc = conf.getInt(MAX_SPLIT_LOCATIONS, 10);
        if (locations.length > max_loc) {
          LOG.warn("Max block location exceeded for split: "
              + split + " splitsize: " + locations.length +
              " maxsize: " + max_loc);
          locations = Arrays.copyOf(locations, max_loc);
        }
        info[i++] = new JobSplit.SplitMetaInfo( 
            locations, offset,
            split.getLength());
        offset += currLen - prevLen;
      }
    }
    return info;
  }

  private static void writeJobSplitMetaInfo(FileSystem fs, Path filename, 
      FsPermission p, int splitMetaInfoVersion, 
      JobSplit.SplitMetaInfo[] allSplitMetaInfo) 
  throws IOException {
    // write the splits meta-info to a file for the job tracker
    FSDataOutputStream out = 
      FileSystem.create(fs, filename, p);
    out.write(JobSplit.META_SPLIT_FILE_HEADER);
    WritableUtils.writeVInt(out, splitMetaInfoVersion);
    WritableUtils.writeVInt(out, allSplitMetaInfo.length);
    for (JobSplit.SplitMetaInfo splitMetaInfo : allSplitMetaInfo) {
      splitMetaInfo.write(out);
    }
    out.close();
  }
}

