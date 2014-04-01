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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * An abstract {@link InputFormat} that returns {@link MultiFileSplit}'s
 * in {@link #getSplits(JobConf, int)} method. Splits are constructed from 
 * the files under the input paths. Each split returned contains <i>nearly</i>
 * equal content length. <br>  
 * Subclasses implement {@link #getRecordReader(InputSplit, JobConf, Reporter)}
 * to construct <code>RecordReader</code>'s for <code>MultiFileSplit</code>'s.
 * @see MultiFileSplit
 * @deprecated Use {@link org.apache.hadoop.mapred.lib.CombineFileInputFormat} instead
 */
@Deprecated
public abstract class MultiFileInputFormat<K, V>
  extends FileInputFormat<K, V> {

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) 
    throws IOException {
    
    Path[] paths = FileUtil.stat2Paths(listStatus(job));
    List<MultiFileSplit> splits = new ArrayList<MultiFileSplit>(Math.min(numSplits, paths.length));
    if (paths.length != 0) {
      // HADOOP-1818: Manage splits only if there are paths
      long[] lengths = new long[paths.length];
      long totLength = 0;
      for(int i=0; i<paths.length; i++) {
        FileSystem fs = paths[i].getFileSystem(job);
        lengths[i] = fs.getContentSummary(paths[i]).getLength();
        totLength += lengths[i];
      }
      double avgLengthPerSplit = ((double)totLength) / numSplits;
      long cumulativeLength = 0;

      int startIndex = 0;

      for(int i=0; i<numSplits; i++) {
        int splitSize = findSize(i, avgLengthPerSplit, cumulativeLength
            , startIndex, lengths);
        if (splitSize != 0) {
          // HADOOP-1818: Manage split only if split size is not equals to 0
          Path[] splitPaths = new Path[splitSize];
          long[] splitLengths = new long[splitSize];
          System.arraycopy(paths, startIndex, splitPaths , 0, splitSize);
          System.arraycopy(lengths, startIndex, splitLengths , 0, splitSize);
          splits.add(new MultiFileSplit(job, splitPaths, splitLengths));
          startIndex += splitSize;
          for(long l: splitLengths) {
            cumulativeLength += l;
          }
        }
      }
    }
    return splits.toArray(new MultiFileSplit[splits.size()]);    
  }

  private int findSize(int splitIndex, double avgLengthPerSplit
      , long cumulativeLength , int startIndex, long[] lengths) {
    
    if(splitIndex == lengths.length - 1)
      return lengths.length - startIndex;
    
    long goalLength = (long)((splitIndex + 1) * avgLengthPerSplit);
    long partialLength = 0;
    // accumulate till just above the goal length;
    for(int i = startIndex; i < lengths.length; i++) {
      partialLength += lengths[i];
      if(partialLength + cumulativeLength >= goalLength) {
        return i - startIndex + 1;
      }
    }
    return lengths.length - startIndex;
  }
  
  @Override
  public abstract RecordReader<K, V> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter)
      throws IOException;
}
