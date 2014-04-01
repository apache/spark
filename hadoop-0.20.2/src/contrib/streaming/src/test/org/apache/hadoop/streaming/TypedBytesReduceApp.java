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

public class TypedBytesReduceApp {

  public void go() throws IOException {
    TypedBytesInput tbinput = new TypedBytesInput(new DataInputStream(System.in));
    TypedBytesOutput tboutput = new TypedBytesOutput(new DataOutputStream(System.out));
    
    Object prevKey = null;
    int sum = 0;
    Object key = tbinput.read();
    while (key != null) {
      if (prevKey != null && !key.equals(prevKey)) {
        tboutput.write(prevKey);  // write key
        tboutput.write(sum);      // write value
        sum = 0;
      }
      sum += (Integer) tbinput.read();
      prevKey = key;
      key = tbinput.read();
    }
    tboutput.write(prevKey);
    tboutput.write(sum);
    
    System.out.flush();
  }

  public static void main(String[] args) throws IOException {
    TypedBytesReduceApp app = new TypedBytesReduceApp();
    app.go();
  }
  
}
