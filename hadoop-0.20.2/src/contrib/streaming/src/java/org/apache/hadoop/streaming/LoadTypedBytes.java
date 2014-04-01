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

import java.io.DataInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.typedbytes.TypedBytesInput;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Utility program that reads typed bytes from standard input and stores them in
 * a sequence file for which the path is given as an argument.
 */
public class LoadTypedBytes implements Tool {

  private Configuration conf;

  public LoadTypedBytes(Configuration conf) {
    this.conf = conf;
  }
  
  public LoadTypedBytes() {
    this(new Configuration());
  }
  
  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * The main driver for <code>LoadTypedBytes</code>.
   */
  public int run(String[] args) throws Exception {
    Path path = new Path(args[0]);
    FileSystem fs = path.getFileSystem(getConf());
    if (fs.exists(path)) {
      System.err.println("given path exists already!");
      return -1;
    }
    TypedBytesInput tbinput = new TypedBytesInput(new DataInputStream(System.in));
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
      TypedBytesWritable.class, TypedBytesWritable.class);
    try {
      TypedBytesWritable key = new TypedBytesWritable();
      TypedBytesWritable value = new TypedBytesWritable();
      byte[] rawKey = tbinput.readRaw();
      while (rawKey != null) {
        byte[] rawValue = tbinput.readRaw();
        key.set(rawKey, 0, rawKey.length);
        value.set(rawValue, 0, rawValue.length);
        writer.append(key, value);
        rawKey = tbinput.readRaw();
      }
    } finally {
      writer.close();
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    LoadTypedBytes loadtb = new LoadTypedBytes();
    int res = ToolRunner.run(loadtb, args);
    System.exit(res);
  }

}
