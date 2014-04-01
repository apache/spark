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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.typedbytes.TypedBytesOutput;
import org.apache.hadoop.typedbytes.TypedBytesWritableOutput;

/**
 * InputWriter that writes the client's input as typed bytes.
 */
public class TypedBytesInputWriter extends InputWriter<Object, Object> {

  private TypedBytesOutput tbOut;
  private TypedBytesWritableOutput tbwOut;

  @Override
  public void initialize(PipeMapRed pipeMapRed) throws IOException {
    super.initialize(pipeMapRed);
    DataOutput clientOut = pipeMapRed.getClientOutput();
    tbOut = new TypedBytesOutput(clientOut);
    tbwOut = new TypedBytesWritableOutput(clientOut);
  }

  @Override
  public void writeKey(Object key) throws IOException {
    writeTypedBytes(key);
  }

  @Override
  public void writeValue(Object value) throws IOException {
    writeTypedBytes(value);
  }
  
  private void writeTypedBytes(Object value) throws IOException {
    if (value instanceof Writable) {
      tbwOut.write((Writable) value);
    } else {
      tbOut.write(value);
    }
  }
  
}
