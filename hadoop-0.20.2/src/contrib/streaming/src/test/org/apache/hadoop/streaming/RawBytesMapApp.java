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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.IntWritable;

public class RawBytesMapApp {
  private String find;
  private DataOutputStream dos;

  public RawBytesMapApp(String find) {
    this.find = find;
    dos = new DataOutputStream(System.out);
  }

  public void go() throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;
    while ((line = in.readLine()) != null) {
      for (String part : line.split(find)) {
        writeString(part);  // write key
        writeInt(1);        // write value
      }
    }
    System.out.flush();
  }
  
  public static void main(String[] args) throws IOException {
    RawBytesMapApp app = new RawBytesMapApp(args[0].replace(".","\\."));
    app.go();
  }
  
  private void writeString(String str) throws IOException {
    byte[] bytes = str.getBytes("UTF-8");
    dos.writeInt(bytes.length);
    dos.write(bytes);
  }
  
  private void writeInt(int i) throws IOException {
    dos.writeInt(4);
    IntWritable iw = new IntWritable(i);
    iw.write(dos);
  }
}
