/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.unsafe;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

class ByteBufferOutputStream extends OutputStream {

  private final ByteBuffer byteBuffer;

  public ByteBufferOutputStream(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  @Override
  public void write(int b) throws IOException {
    byteBuffer.put((byte) b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    byteBuffer.put(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    byteBuffer.put(b, off, len);
  }
}
