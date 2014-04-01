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
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.typedbytes.TypedBytesInput;
import org.apache.hadoop.typedbytes.TypedBytesOutput;

public class TypedBytesMapApp {

  private String find;

  public TypedBytesMapApp(String find) {
    this.find = find;
  }

  public void go() throws IOException {
    TypedBytesInput tbinput = new TypedBytesInput(new DataInputStream(System.in));
    TypedBytesOutput tboutput = new TypedBytesOutput(new DataOutputStream(System.out));

    Object key = tbinput.readRaw();
    while (key != null) {
      Object value = tbinput.read();
      for (String part : value.toString().split(find)) {
        tboutput.write(part);  // write key
        tboutput.write(1);     // write value
      }
      System.err.println("reporter:counter:UserCounters,InputLines,1");
      key = tbinput.readRaw();
    }
    
    System.out.flush();
  }
  
  public static void main(String[] args) throws IOException {
    TypedBytesMapApp app = new TypedBytesMapApp(args[0].replace(".","\\."));
    app.go();
  }
  
}
