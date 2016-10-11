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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ByteArrayReadableChannel implements ReadableByteChannel {
  private final Logger logger = LoggerFactory.getLogger(ByteArrayReadableChannel.class);

  private byte[] data;
  private int offset;

  public byte[] getData() {
    return data;
  }

  public int length() {
    return data.length;
  }

  public void reset() {
    offset = 0;
    data = null;
  }

  public ByteArrayReadableChannel() {

  }

  public void feedData(ByteBuf buf) {
    int length = buf.readableBytes();
    if (buf.hasArray()) {
      data = buf.array();
      buf.skipBytes(length);
    } else {
      data = new byte[length];
      buf.readBytes(data);
    }
    logger.debug("ByteReadableChannel: get {} bytes", data.length);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    if (offset == data.length) {
      logger.debug("channel empty");
      return -1;
    }

    int toPut = Math.min(length()-offset, dst.remaining());
    dst.put(data, offset, toPut);
    offset += toPut;

    return toPut;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public boolean isOpen() {
    return true;
  }
}