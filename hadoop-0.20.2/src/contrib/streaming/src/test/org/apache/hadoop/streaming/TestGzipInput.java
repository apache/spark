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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * This class tests gzip input streaming in MapReduce local mode.
 */
public class TestGzipInput extends TestStreaming
{

  public TestGzipInput() throws IOException {
    INPUT_FILE = new File(TEST_DIR, "input.txt.gz");
  }
  
  protected void createInput() throws IOException
  {
    assertTrue("Creating " + TEST_DIR, TEST_DIR.mkdirs());
    GZIPOutputStream out = new GZIPOutputStream(
                                                new FileOutputStream(INPUT_FILE.getAbsoluteFile()));
    out.write(input.getBytes("UTF-8"));
    out.close();
  }


  protected String[] genArgs() {
    return new String[] {
      "-input", INPUT_FILE.getAbsolutePath(),
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-mapper", map,
      "-reducer", reduce,
      "-jobconf", "stream.recordreader.compression=gzip"
    };
    
  }

  public static void main(String[]args) throws Exception
  {
    new TestGzipInput().testCommandLine();
  }

}
