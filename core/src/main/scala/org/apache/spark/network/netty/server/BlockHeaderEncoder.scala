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

package org.apache.spark.network.netty.server

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder

/**
 * A simple encoder for BlockHeader. See [[BlockServer]] for the server to client protocol.
 */
private[server]
class BlockHeaderEncoder extends MessageToByteEncoder[BlockHeader] {
  override def encode(ctx: ChannelHandlerContext, msg: BlockHeader, out: ByteBuf): Unit = {
    // message = message length (4 bytes) + block id length (4 bytes) + block id + block data
    // message length = block id length (4 bytes) + size of block id + size of block data
    val blockIdBytes = msg.blockId.getBytes
    msg.error match {
      case Some(errorMsg) =>
        val errorBytes = errorMsg.getBytes
        out.writeInt(4 + blockIdBytes.length + errorBytes.size)
        out.writeInt(-blockIdBytes.length)  // use negative block id length to represent errors
        out.writeBytes(blockIdBytes)  // next is blockId itself
        out.writeBytes(errorBytes)  // error message
      case None =>
        out.writeInt(4 + blockIdBytes.length + msg.blockSize)
        out.writeInt(blockIdBytes.length)  // First 4 bytes is blockId length
        out.writeBytes(blockIdBytes)  // next is blockId itself
        // msg of size blockSize will be written by ServerHandler
    }
  }
}
