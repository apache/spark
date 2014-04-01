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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

/**
 * A sub-collection of input files. Unlike {@link FileSplit}, MultiFileSplit 
 * class does not represent a split of a file, but a split of input files 
 * into smaller sets. The atomic unit of split is a file. <br> 
 * MultiFileSplit can be used to implement {@link RecordReader}'s, with 
 * reading one record per file.
 * @see FileSplit
 * @see MultiFileInputFormat 
 * @deprecated Use {@link org.apache.hadoop.mapred.lib.CombineFileSplit} instead
 */
@Deprecated
public class MultiFileSplit extends CombineFileSplit {

  MultiFileSplit() {}
  
  public MultiFileSplit(JobConf job, Path[] files, long[] lengths) {
    super(job, files, lengths);
  }

  public String[] getLocations() throws IOException {
    HashSet<String> hostSet = new HashSet<String>();
    for (Path file : getPaths()) {
      FileSystem fs = file.getFileSystem(getJob());
      FileStatus status = fs.getFileStatus(file);
      BlockLocation[] blkLocations = fs.getFileBlockLocations(status,
                                          0, status.getLen());
      if (blkLocations != null && blkLocations.length > 0) {
        addToSet(hostSet, blkLocations[0].getHosts());
      }
    }
    return hostSet.toArray(new String[hostSet.size()]);
  }

  private void addToSet(Set<String> set, String[] array) {
    for(String s:array)
      set.add(s); 
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for(int i=0; i < getPaths().length; i++) {
      sb.append(getPath(i).toUri().getPath() + ":0+" + getLength(i));
      if (i < getPaths().length -1) {
        sb.append("\n");
      }
    }

    return sb.toString();
  }
}

