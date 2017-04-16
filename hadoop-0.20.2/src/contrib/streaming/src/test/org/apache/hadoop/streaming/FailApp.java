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
 * A simple Java app that will consume all input from stdin, echoing
 * it to stdout, and then optionally throw an exception (which should
 * cause a non-zero exit status for the process).
 */
public class FailApp
{

  public FailApp() {
  }

  public void go(boolean fail) throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;

    // Consume all input (to make sure streaming will still count this
    // task as failed even if all input was consumed).
    while ((line = in.readLine()) != null) {
      System.out.println(line);
    }

    if (fail) {
      throw new RuntimeException("Intentionally failing task");
    }
  }

  public static void main(String[] args) throws IOException {
    boolean fail = true;
    if (args.length >= 1 && "false".equals(args[0])) {
      fail = false;
    }
    
    FailApp app = new FailApp();
    app.go(fail);
  }
}
