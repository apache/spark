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

package org.apache.hadoop.streaming.io;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.typedbytes.TypedBytesInput;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

/**
 * OutputReader that reads the client's output as typed bytes.
 */
public class TypedBytesOutputReader extends 
  OutputReader<TypedBytesWritable, TypedBytesWritable> {

  private byte[] bytes;
  private DataInput clientIn;
  private TypedBytesWritable key;
  private TypedBytesWritable value;
  private TypedBytesInput in;
  
  @Override
  public void initialize(PipeMapRed pipeMapRed) throws IOException {
    super.initialize(pipeMapRed);
    clientIn = pipeMapRed.getClientInput();
    key = new TypedBytesWritable();
    value = new TypedBytesWritable();
    in = new TypedBytesInput(clientIn);
  }
  
  @Override
  public boolean readKeyValue() throws IOException {
    bytes = in.readRaw();
    if (bytes == null) {
      return false;
    }
    key.set(bytes, 0, bytes.length);
    bytes = in.readRaw();
    value.set(bytes, 0, bytes.length);
    return true;
  }
  
  @Override
  public TypedBytesWritable getCurrentKey() throws IOException {
    return key;
  }
  
  @Override
  public TypedBytesWritable getCurrentValue() throws IOException {
    return value;
  }

  @Override
  public String getLastOutput() {
    if (bytes != null) {
      return new TypedBytesWritable(bytes).toString();
    } else {
      return null;
    }
  }

}
