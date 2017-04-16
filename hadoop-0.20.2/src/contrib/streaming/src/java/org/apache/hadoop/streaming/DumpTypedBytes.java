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

package org.apache.hadoop.streaming;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.typedbytes.TypedBytesOutput;
import org.apache.hadoop.typedbytes.TypedBytesWritableOutput;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Utility program that fetches all files that match a given pattern and dumps
 * their content to stdout as typed bytes. This works for all files that can be
 * handled by {@link org.apache.hadoop.streaming.AutoInputFormat}.
 */
public class DumpTypedBytes implements Tool {

  private Configuration conf;

  public DumpTypedBytes(Configuration conf) {
    this.conf = conf;
  }
  
  public DumpTypedBytes() {
    this(new Configuration());
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * The main driver for <code>DumpTypedBytes</code>.
   */
  public int run(String[] args) throws Exception {
    Path pattern = new Path(args[0]);
    FileSystem fs = pattern.getFileSystem(getConf());
    fs.setVerifyChecksum(true);
    for (Path p : FileUtil.stat2Paths(fs.globStatus(pattern), pattern)) {
      List<FileStatus> inputFiles = new ArrayList<FileStatus>();
      FileStatus status = fs.getFileStatus(p);
      if (status.isDir()) {
        FileStatus[] files = fs.listStatus(p);
        Collections.addAll(inputFiles, files);
      } else {
        inputFiles.add(status);
      }
      return dumpTypedBytes(inputFiles);
    }
    return -1;
  }

  /**
   * Dump given list of files to standard output as typed bytes.
   */
  @SuppressWarnings("unchecked")
  private int dumpTypedBytes(List<FileStatus> files) throws IOException {
    JobConf job = new JobConf(getConf()); 
    DataOutputStream dout = new DataOutputStream(System.out);
    AutoInputFormat autoInputFormat = new AutoInputFormat();
    for (FileStatus fileStatus : files) {
      FileSplit split = new FileSplit(fileStatus.getPath(), 0,
        fileStatus.getLen() * fileStatus.getBlockSize(),
        (String[]) null);
      RecordReader recReader = null;
      try {
        recReader = autoInputFormat.getRecordReader(split, job, Reporter.NULL);
        Object key = recReader.createKey();
        Object value = recReader.createValue();
        while (recReader.next(key, value)) {
          if (key instanceof Writable) {
            TypedBytesWritableOutput.get(dout).write((Writable) key);
          } else {
            TypedBytesOutput.get(dout).write(key);
          }
          if (value instanceof Writable) {
            TypedBytesWritableOutput.get(dout).write((Writable) value);
          } else {
            TypedBytesOutput.get(dout).write(value);
          }
        }
      } finally {
        if (recReader != null) {
          recReader.close();
        }
      }
    }
    dout.flush();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    DumpTypedBytes dumptb = new DumpTypedBytes();
    int res = ToolRunner.run(dumptb, args);
    System.exit(res);
  }
  
}
