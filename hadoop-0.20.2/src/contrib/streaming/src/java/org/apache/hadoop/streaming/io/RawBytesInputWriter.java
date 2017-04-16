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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.streaming.PipeMapRed;

/**
 * InputWriter that writes the client's input as raw bytes.
 */
public class RawBytesInputWriter extends InputWriter<Writable, Writable> {

  private DataOutput clientOut;
  private ByteArrayOutputStream bufferOut;
  private DataOutputStream bufferDataOut;

  @Override
  public void initialize(PipeMapRed pipeMapRed) throws IOException {
    super.initialize(pipeMapRed);
    clientOut = pipeMapRed.getClientOutput();
    bufferOut = new ByteArrayOutputStream();
    bufferDataOut = new DataOutputStream(bufferOut);
  }
  
  @Override
  public void writeKey(Writable key) throws IOException {
    writeRawBytes(key);
  }

  @Override
  public void writeValue(Writable value) throws IOException {
    writeRawBytes(value);
  }

  private void writeRawBytes(Writable writable) throws IOException {
    if (writable instanceof BytesWritable) {
      BytesWritable bw = (BytesWritable) writable;
      byte[] bytes = bw.getBytes();
      int length = bw.getLength();
      clientOut.writeInt(length);
      clientOut.write(bytes, 0, length);
    } else {
      bufferOut.reset();
      writable.write(bufferDataOut);
      byte[] bytes = bufferOut.toByteArray();
      clientOut.writeInt(bytes.length);
      clientOut.write(bytes);
    }
  }
  
}
