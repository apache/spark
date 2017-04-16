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
 *  The UlimitApp discards the input
 *  and exec's ulimit -v to know the ulimit value.
 *  And writes the output to the standard out. 
 *  @see {@link TestUlimit}
 */
public class UlimitApp {
  public static void main(String args[]) throws IOException{
    BufferedReader in = new BufferedReader(
                            new InputStreamReader(System.in));
    String line = null;
    while ((line = in.readLine()) != null) {}

    Process process = Runtime.getRuntime().exec(new String[]{
                                 "bash", "-c", "ulimit -v"});
    InputStream is = process.getInputStream();
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);
    while ((line = br.readLine()) != null) {
      System.out.println(line);
    }
  }
}
