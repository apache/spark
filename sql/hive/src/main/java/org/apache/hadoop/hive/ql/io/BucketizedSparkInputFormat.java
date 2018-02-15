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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * A {@link InputFormat} implementation for reading bucketed tables.
 *
 * We cannot directly use {@link BucketizedHiveInputFormat} from Hive as it depends on the
 * map-reduce plan to get required information for split generation.
 */
public class BucketizedSparkInputFormat<K extends WritableComparable, V extends Writable>
        extends BucketizedHiveInputFormat<K, V> {

  private static final String FILE_INPUT_FORMAT = "file.inputformat";

  @Override
  public RecordReader getRecordReader(
      InputSplit split,
      JobConf job,
      Reporter reporter) throws IOException {

    BucketizedHiveInputSplit hsplit = (BucketizedHiveInputSplit) split;
    String inputFormatClassName = null;
    Class inputFormatClass = null;

    try {
      inputFormatClassName = hsplit.inputFormatClassName();
      inputFormatClass = job.getClassByName(inputFormatClassName);
    } catch (ClassNotFoundException e) {
      throw new IOException("Cannot find class " + inputFormatClassName, e);
    }

    InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
    return new BucketizedSparkRecordReader<>(inputFormat, hsplit, job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numBuckets) throws IOException {
    final String inputFormatClassName = job.get(FILE_INPUT_FORMAT);
    final String[] inputDirs = job.get(INPUT_DIR).split(StringUtils.COMMA_STR);

    if (inputDirs.length != 1) {
      throw new IOException(this.getClass().getCanonicalName() +
        " expects only one input directory. " + inputDirs.length +
        " directories detected : " + Arrays.toString(inputDirs));
    }

    final String inputDir = inputDirs[0];
    final Path inputPath = new Path(inputDir);
    final JobConf newJob = new JobConf(job);
    final FileStatus[] listStatus = this.listStatus(newJob, inputPath);
    final InputSplit[] result = new InputSplit[numBuckets];

    if (listStatus.length != 0 && listStatus.length != numBuckets) {
      throw new IOException("Bucketed path was expected to have " + numBuckets + " files but " +
        listStatus.length + " files are present. Path = " + inputPath);
    }

    try {
      final Class<?> inputFormatClass = Class.forName(inputFormatClassName);
      final InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
      newJob.setInputFormat(inputFormat.getClass());

      for (int i = 0; i < numBuckets; i++) {
        final FileStatus fileStatus = listStatus[i];
        FileInputFormat.setInputPaths(newJob, fileStatus.getPath());

        final InputSplit[] inputSplits = inputFormat.getSplits(newJob, 0);
        if (inputSplits != null && inputSplits.length > 0) {
          result[i] =
            new BucketizedHiveInputSplit(inputSplits, inputFormatClass.getName());
        }
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Unable to find the InputFormat class " + inputFormatClassName, e);
    }
    return result;
  }
}
