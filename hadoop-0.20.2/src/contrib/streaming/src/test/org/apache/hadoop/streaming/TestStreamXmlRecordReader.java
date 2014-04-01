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

import org.apache.hadoop.fs.FileUtil;

/**
 * This class tests StreamXmlRecordReader
 * The test creates an XML file, uses StreamXmlRecordReader and compares
 * the expected output against the generated output
 */
public class TestStreamXmlRecordReader extends TestStreaming
{

  private StreamJob job;

  public TestStreamXmlRecordReader() throws IOException {
    INPUT_FILE = new File("input.xml");
    input = "<xmltag>\t\nroses.are.red\t\nviolets.are.blue\t\nbunnies.are.pink\t\n</xmltag>\t\n";
  }
  
  protected void createInput() throws IOException
  {
    FileOutputStream out = new FileOutputStream(INPUT_FILE.getAbsoluteFile());
    String dummyXmlStartTag = "<PATTERN>\n";
    String dummyXmlEndTag = "</PATTERN>\n";
    out.write(dummyXmlStartTag.getBytes("UTF-8"));
    out.write(input.getBytes("UTF-8"));
    out.write(dummyXmlEndTag.getBytes("UTF-8"));
    out.close();
  }

  protected String[] genArgs() {
    return new String[] {
      "-input", INPUT_FILE.getAbsolutePath(),
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-mapper","cat", 
      "-reducer", "NONE", 
      "-inputreader", "StreamXmlRecordReader,begin=<xmltag>,end=</xmltag>"
    };
  }

  public void testCommandLine() throws IOException {
    try {
      try {
        FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
      } catch (Exception e) {
      }
      createInput();
      job = new StreamJob(genArgs(), false);
      job.go();
      File outFile = new File(OUTPUT_DIR, "part-00000").getAbsoluteFile();
      String output = StreamUtil.slurp(outFile);
      outFile.delete();
      assertEquals(input, output);
    } finally {
      try {
        INPUT_FILE.delete();
        FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[]args) throws Exception
  {
    new TestStreamXmlRecordReader().testCommandLine();
  }
}
