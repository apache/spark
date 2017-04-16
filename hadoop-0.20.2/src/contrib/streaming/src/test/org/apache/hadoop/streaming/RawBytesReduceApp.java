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
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

public class RawBytesReduceApp {
  private DataInputStream dis;

  public RawBytesReduceApp() {
    dis = new DataInputStream(System.in);
  }
  
  public void go() throws IOException {
    String prevKey = null;
    int sum = 0;
    String key = readString();
    while (key != null) {
      if (prevKey != null && !key.equals(prevKey)) {
        System.out.println(prevKey + "\t" + sum);
        sum = 0;
      }
      sum += readInt();
      prevKey = key;
      key = readString();
    }
    System.out.println(prevKey + "\t" + sum);
    System.out.flush();
  }

  public static void main(String[] args) throws IOException {
    RawBytesReduceApp app = new RawBytesReduceApp();
    app.go();
  }
  
  private String readString() throws IOException {
    int length;
    try {
      length = dis.readInt();
    } catch (EOFException eof) {
      return null;
    }
    byte[] bytes = new byte[length];
    dis.readFully(bytes);
    return new String(bytes, "UTF-8");
  }
  
  private int readInt() throws IOException {
    dis.readInt(); // ignore (we know it's 4)
    IntWritable iw = new IntWritable();
    iw.readFields(dis);
    return iw.get();
  }
}
