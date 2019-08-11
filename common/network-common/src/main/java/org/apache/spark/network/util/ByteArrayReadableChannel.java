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

package org.apache.spark.network.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import io.netty.buffer.ByteBuf;

public class ByteArrayReadableChannel implements ReadableByteChannel {
  private ByteBuf data;

  public int readableBytes() {
    return data.readableBytes();
  }

  public void feedData(ByteBuf buf) {
    data = buf;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int totalRead = 0;
    while (data.readableBytes() > 0 && dst.remaining() > 0) {
      int bytesToRead = Math.min(data.readableBytes(), dst.remaining());
      dst.put(data.readSlice(bytesToRead).nioBuffer());
      totalRead += bytesToRead;
    }

    if (data.readableBytes() == 0) {
      data.release();
    }

    return totalRead;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public boolean isOpen() {
    return true;
  }

}
