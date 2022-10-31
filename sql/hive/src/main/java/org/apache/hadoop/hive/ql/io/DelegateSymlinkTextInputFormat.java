/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Delegate for SymlinkTextInputFormat, created to address SPARK-40815.
 * Fixes an issue where SymlinkTextInputFormat returns empty splits which could result in
 * the correctness issue when "spark.hadoopRDD.ignoreEmptySplits" is enabled.
 * <p>
 * In this class, we update the split start and length to match the target file input thus fixing
 * the issue.
 */
public class DelegateSymlinkTextInputFormat extends SymlinkTextInputFormat {

  public static class DelegateSymlinkTextInputSplit extends FileSplit {
    private Path targetPath; // Path to the actual data file, not the symlink file.

    // Used for deserialisation.
    public DelegateSymlinkTextInputSplit() {
      super((Path) null, 0, 0, (String[]) null);
      targetPath = null;
    }

    public DelegateSymlinkTextInputSplit(SymlinkTextInputSplit split) throws IOException {
      // It is fine to set start and length to the target file split because
      // SymlinkTextInputFormat maintains 1-1 mapping between SymlinkTextInputSplit and FileSplit.
      super(split.getPath(),
        split.getTargetSplit().getStart(),
        split.getTargetSplit().getLength(),
        split.getTargetSplit().getLocations());
      this.targetPath = split.getTargetSplit().getPath();
    }

    /**
     * Returns target path.
     * Visible for testing.
     */
    public Path getTargetPath() {
      return targetPath;
    }

    /**
     * Reconstructs the delegate input split.
     */
    private SymlinkTextInputSplit getSplit() throws IOException {
      return new SymlinkTextInputSplit(
        getPath(),
        new FileSplit(targetPath, getStart(), getLength(), getLocations())
      );
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeString(out, (this.targetPath != null) ? this.targetPath.toString() : "");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      String target = Text.readString(in);
      this.targetPath = (!target.isEmpty()) ? new Path(target) : null;
    }
  }

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    InputSplit targetSplit = ((DelegateSymlinkTextInputSplit) split).getSplit();
    return super.getRecordReader(targetSplit, job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] splits = super.getSplits(job, numSplits);
    for (int i = 0; i < splits.length; i++) {
      SymlinkTextInputSplit split = (SymlinkTextInputSplit) splits[i];
      splits[i] = new DelegateSymlinkTextInputSplit(split);
    }
    return splits;
  }

  @Override
  public void configure(JobConf job) {
    super.configure(job);
  }

  @Override
  public ContentSummary getContentSummary(Path p, JobConf job) throws IOException {
    return super.getContentSummary(p, job);
  }
}
