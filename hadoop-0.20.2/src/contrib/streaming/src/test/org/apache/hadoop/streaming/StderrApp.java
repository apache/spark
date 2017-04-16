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

/**
 * Output an arbitrary number of stderr lines before or after
 * consuming the keys/values from stdin.
 */
public class StderrApp
{
  /**
   * Print preWriteLines to stderr, pausing sleep ms between each
   * output, then consume stdin and echo it to stdout, then write
   * postWriteLines to stderr.
   */
  public static void go(int preWriteLines, int sleep, int postWriteLines) throws IOException {
    go(preWriteLines, sleep, postWriteLines, false);
  }
  
  public static void go(int preWriteLines, int sleep, int postWriteLines, boolean status) throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;
    
    if (status) {
      System.err.println("reporter:status:starting echo");
    }      
       
    while (preWriteLines > 0) {
      --preWriteLines;
      System.err.println("some stderr output before reading input, "
                         + preWriteLines + " lines remaining, sleeping " + sleep);
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {}
    }
    
    while ((line = in.readLine()) != null) {
      System.out.println(line);
    }
    
    while (postWriteLines > 0) {
      --postWriteLines;
      System.err.println("some stderr output after reading input, lines remaining "
                         + postWriteLines);
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      System.err.println("Usage: StderrApp PREWRITE SLEEP POSTWRITE [STATUS]");
      return;
    }
    int preWriteLines = Integer.parseInt(args[0]);
    int sleep = Integer.parseInt(args[1]);
    int postWriteLines = Integer.parseInt(args[2]);
    boolean status = args.length > 3 ? Boolean.parseBoolean(args[3]) : false;
    
    go(preWriteLines, sleep, postWriteLines, status);
  }
}
