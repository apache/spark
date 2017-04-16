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

import java.io.*;

import org.apache.hadoop.streaming.Environment;

/** A minimal Java implementation of /usr/bin/tr.
    Used to test the usage of external applications without adding
    platform-specific dependencies.
 */
public class TrAppReduce
{

  public TrAppReduce(char find, char replace)
  {
    this.find = find;
    this.replace = replace;
  }

  void testParentJobConfToEnvVars() throws IOException
  {
    env = new Environment();
    // test that some JobConf properties are exposed as expected     
    // Note the dots translated to underscore: 
    // property names have been escaped in PipeMapRed.safeEnvVarName()
    expect("mapred_job_tracker", "local");
    //expect("mapred_local_dir", "build/test/mapred/local");
    expectDefined("mapred_local_dir");
    expect("mapred_output_format_class", "org.apache.hadoop.mapred.TextOutputFormat");
    expect("mapred_output_key_class", "org.apache.hadoop.io.Text");
    expect("mapred_output_value_class", "org.apache.hadoop.io.Text");

    expect("mapred_task_is_map", "false");
    expectDefined("mapred_task_id");

    expectDefined("io_sort_factor");

    // the FileSplit context properties are not available in local hadoop..
    // so can't check them in this test.

  }

  // this runs in a subprocess; won't use JUnit's assertTrue()    
  void expect(String evName, String evVal) throws IOException
  {
    String got = env.getProperty(evName);
    if (!evVal.equals(got)) {
      String msg = "FAIL evName=" + evName + " got=" + got + " expect=" + evVal;
      throw new IOException(msg);
    }
  }

  void expectDefined(String evName) throws IOException
  {
    String got = env.getProperty(evName);
    if (got == null) {
      String msg = "FAIL evName=" + evName + " is undefined. Expect defined.";
      throw new IOException(msg);
    }
  }

  public void go() throws IOException
  {
    testParentJobConfToEnvVars();
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;

    while ((line = in.readLine()) != null) {
      String out = line.replace(find, replace);
      System.out.println(out);
    }
  }

  public static void main(String[] args) throws IOException
  {
    args[0] = CUnescape(args[0]);
    args[1] = CUnescape(args[1]);
    TrAppReduce app = new TrAppReduce(args[0].charAt(0), args[1].charAt(0));
    app.go();
  }

  public static String CUnescape(String s)
  {
    if (s.equals("\\n")) {
      return "\n";
    } else {
      return s;
    }
  }
  char find;
  char replace;
  Environment env;
}
