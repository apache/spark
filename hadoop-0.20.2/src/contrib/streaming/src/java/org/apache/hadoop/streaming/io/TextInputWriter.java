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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.streaming.PipeMapRed;

/**
 * InputWriter that writes the client's input as text.
 */
public class TextInputWriter extends InputWriter<Object, Object> {
  
  private DataOutput clientOut;
  private byte[] inputSeparator;
  
  @Override
  public void initialize(PipeMapRed pipeMapRed) throws IOException {
    super.initialize(pipeMapRed);
    clientOut = pipeMapRed.getClientOutput();
    inputSeparator = pipeMapRed.getInputSeparator();
  }
  
  @Override
  public void writeKey(Object key) throws IOException {
    writeUTF8(key);
    clientOut.write(inputSeparator);
  }

  @Override
  public void writeValue(Object value) throws IOException {
    writeUTF8(value);
    clientOut.write('\n');
  }
  
  // Write an object to the output stream using UTF-8 encoding
  private void writeUTF8(Object object) throws IOException {
    byte[] bval;
    int valSize;
    if (object instanceof BytesWritable) {
      BytesWritable val = (BytesWritable) object;
      bval = val.getBytes();
      valSize = val.getLength();
    } else if (object instanceof Text) {
      Text val = (Text) object;
      bval = val.getBytes();
      valSize = val.getLength();
    } else {
      String sval = object.toString();
      bval = sval.getBytes("UTF-8");
      valSize = bval.length;
    }
    clientOut.write(bval, 0, valSize);
  }
  
}
