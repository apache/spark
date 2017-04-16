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

/** 
    Used to test the usage of external applications without adding
    platform-specific dependencies.
 */
public class StreamAggregate extends TrApp
{

  public StreamAggregate()
  {
    super('.', ' ');
  }

  public void go() throws IOException
  {
    testParentJobConfToEnvVars();
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;

    while ((line = in.readLine()) != null) {
      String [] words = line.split(" ");
      for (int i = 0; i< words.length; i++) {
        String out = "LongValueSum:" + words[i].trim() + "\t" + "1";
        System.out.println(out);
      }
    }
  }

  public static void main(String[] args) throws IOException
  {
    TrApp app = new StreamAggregate();
    app.go();
  }
}
