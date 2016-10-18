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
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ByteArrayReadableChannel implements ReadableByteChannel {
  private final Logger logger = LoggerFactory.getLogger(ByteArrayReadableChannel.class);

  private byte[] backArray;
  private ByteBuf data;

  public int length() {
    return data.readableBytes();
  }

  public void reset() {
    data.clear();
  }

  public ByteArrayReadableChannel(int size) {
    backArray = new byte[size];
    data = Unpooled.wrappedBuffer(backArray);
    data.clear();
  }

  public void feedData(ByteBuf buf) {
    int toFeed = Math.min(data.writableBytes(), buf.readableBytes());
    buf.readBytes(data, toFeed);
    logger.debug("ByteReadableChannel: get {} bytes", toFeed);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int toPut = Math.min(data.readableBytes(), dst.remaining());
    dst.put(backArray, data.readerIndex(), toPut);
    data.skipBytes(toPut);
    return toPut;
  }

  @Override
  public void close() throws IOException {
    data.release();
  }

  @Override
  public boolean isOpen() {
    return true;
  }

}
