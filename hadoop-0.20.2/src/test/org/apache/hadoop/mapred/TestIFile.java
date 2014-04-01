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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;

import org.junit.Test;

public class TestIFile {

  @Test
  /**
   * Create an IFile.Writer using GzipCodec since this codec does not
   * have a compressor when run via the tests (ie no native libraries).
   */
  public void testIFileWriterWithCodec() throws Exception {
    Configuration conf = new Configuration();
    FileSystem localFs = FileSystem.getLocal(conf);
    FileSystem rfs = ((LocalFileSystem)localFs).getRaw();
    Path path = new Path(new Path("build/test.ifile"), "data");
    DefaultCodec codec = new GzipCodec();
    codec.setConf(conf);
    IFile.Writer<Text, Text> writer =
      new IFile.Writer<Text, Text>(conf, rfs, path, Text.class, Text.class,
                                   codec, null);
    writer.close();
  }

  @Test
  /** Same as above but create a reader. */
  public void testIFileReaderWithCodec() throws Exception {
    Configuration conf = new Configuration();
    FileSystem localFs = FileSystem.getLocal(conf);
    FileSystem rfs = ((LocalFileSystem)localFs).getRaw();
    Path path = new Path(new Path("build/test.ifile"), "data");
    DefaultCodec codec = new GzipCodec();
    codec.setConf(conf);
    IFile.Reader<Text, Text> reader =
      new IFile.Reader<Text, Text>(conf, rfs, path, codec, null);
    reader.close();
  }
}
