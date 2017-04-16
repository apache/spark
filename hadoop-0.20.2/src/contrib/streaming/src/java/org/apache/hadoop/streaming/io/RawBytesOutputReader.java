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
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.streaming.PipeMapRed;

/**
 * OutputReader that reads the client's output as raw bytes.
 */
public class RawBytesOutputReader 
  extends OutputReader<BytesWritable, BytesWritable> {

  private DataInput clientIn;
  private byte[] bytes;
  private BytesWritable key;
  private BytesWritable value;

  @Override
  public void initialize(PipeMapRed pipeMapRed) throws IOException {
    super.initialize(pipeMapRed);
    clientIn = pipeMapRed.getClientInput();
    key = new BytesWritable();
    value = new BytesWritable();
  }
  
  @Override
  public boolean readKeyValue() throws IOException {
    int length = readLength();
    if (length < 0) {
      return false;
    }
    key.set(readBytes(length), 0, length);
    length = readLength();
    value.set(readBytes(length), 0, length);
    return true;
  }
  
  @Override
  public BytesWritable getCurrentKey() throws IOException {
    return key;
  }
  
  @Override
  public BytesWritable getCurrentValue() throws IOException {
    return value;
  }

  @Override
  public String getLastOutput() {
    if (bytes != null) {
      return new BytesWritable(bytes).toString();
    } else {
      return null;
    }
  }

  private int readLength() throws IOException {
    try {
      return clientIn.readInt();
    } catch (EOFException eof) {
      return -1;
    }
  }
  
  private byte[] readBytes(int length) throws IOException {
    bytes = new byte[length];
    clientIn.readFully(bytes);
    return bytes;
  }
  
}
