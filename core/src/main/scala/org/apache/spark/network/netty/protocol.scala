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

package org.apache.spark.network.netty

import java.util.{List => JList}

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelHandler.Sharable
import io.netty.handler.codec._

import org.apache.spark.Logging
import org.apache.spark.network.{NettyManagedBuffer, ManagedBuffer}


/** Messages from the client to the server. */
private[netty]
sealed trait ClientRequest {
  def id: Byte
}

/**
 * Request to fetch a sequence of blocks from the server. A single [[BlockFetchRequest]] can
 * correspond to multiple [[ServerResponse]]s.
 */
private[netty]
final case class BlockFetchRequest(blocks: Seq[String]) extends ClientRequest {
  override def id = 0
}

/**
 * Request to upload a block to the server. Currently the server does not ack the upload request.
 */
private[netty]
final case class BlockUploadRequest(blockId: String, data: ManagedBuffer) extends ClientRequest {
  require(blockId.length <= Byte.MaxValue)
  override def id = 1
}


/** Messages from server to client (usually in response to some [[ClientRequest]]. */
private[netty]
sealed trait ServerResponse {
  def id: Byte
}

/** Response to [[BlockFetchRequest]] when a block exists and has been successfully fetched. */
private[netty]
final case class BlockFetchSuccess(blockId: String, data: ManagedBuffer) extends ServerResponse {
  require(blockId.length <= Byte.MaxValue)
  override def id = 0
}

/** Response to [[BlockFetchRequest]] when there is an error fetching the block. */
private[netty]
final case class BlockFetchFailure(blockId: String, error: String) extends ServerResponse {
  require(blockId.length <= Byte.MaxValue)
  override def id = 1
}


/**
 * Encoder for [[ClientRequest]] used in client side.
 *
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@Sharable
private[netty]
final class ClientRequestEncoder extends MessageToMessageEncoder[ClientRequest] {
  override def encode(ctx: ChannelHandlerContext, in: ClientRequest, out: JList[Object]): Unit = {
    in match {
      case BlockFetchRequest(blocks) =>
        // 8 bytes: frame size
        // 1 byte: BlockFetchRequest vs BlockUploadRequest
        // 4 byte: num blocks
        // then for each block id write 1 byte for blockId.length and then blockId itself
        val frameLength = 8 + 1 + 4 + blocks.size + blocks.map(_.size).fold(0)(_ + _)
        val buf = ctx.alloc().buffer(frameLength)

        buf.writeLong(frameLength)
        buf.writeByte(in.id)
        buf.writeInt(blocks.size)
        blocks.foreach { blockId =>
          ProtocolUtils.writeBlockId(buf, blockId)
        }

        assert(buf.writableBytes() == 0)
        out.add(buf)

      case BlockUploadRequest(blockId, data) =>
        // 8 bytes: frame size
        // 1 byte: msg id (BlockFetchRequest vs BlockUploadRequest)
        // 1 byte: blockId.length
        // data itself (length can be derived from: frame size - 1 - blockId.length)
        val headerLength = 8 + 1 + 1 + blockId.length
        val frameLength = headerLength + data.size
        val header = ctx.alloc().buffer(headerLength)

        // Call this before we add header to out so in case of exceptions
        // we don't send anything at all.
        val body = data.convertToNetty()

        header.writeLong(frameLength)
        header.writeByte(in.id)
        ProtocolUtils.writeBlockId(header, blockId)

        assert(header.writableBytes() == 0)
        out.add(header)
        out.add(body)
    }
  }
}


/**
 * Decoder in the server side to decode client requests.
 * This decoder is stateless so it is safe to be shared by multiple threads.
 *
 * This assumes the inbound messages have been processed by a frame decoder created by
 * [[ProtocolUtils.createFrameDecoder()]].
 */
@Sharable
private[netty]
final class ClientRequestDecoder extends MessageToMessageDecoder[ByteBuf] {
  override protected def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: JList[AnyRef]): Unit =
  {
    val msgTypeId = in.readByte()
    val decoded = msgTypeId match {
      case 0 =>  // BlockFetchRequest
        val numBlocks = in.readInt()
        val blockIds = Seq.fill(numBlocks) { ProtocolUtils.readBlockId(in) }
        BlockFetchRequest(blockIds)

      case 1 =>  // BlockUploadRequest
        val blockId = ProtocolUtils.readBlockId(in)
        in.retain()  // retain the bytebuf so we don't recycle it immediately.
        BlockUploadRequest(blockId, new NettyManagedBuffer(in))
    }

    assert(decoded.id == msgTypeId)
    out.add(decoded)
  }
}


/**
 * Encoder used by the server side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@Sharable
private[netty]
final class ServerResponseEncoder extends MessageToMessageEncoder[ServerResponse] with Logging {
  override def encode(ctx: ChannelHandlerContext, in: ServerResponse, out: JList[Object]): Unit = {
    in match {
      case BlockFetchSuccess(blockId, data) =>
        // Handle the body first so if we encounter an error getting the body, we can respond
        // with an error instead.
        var body: AnyRef = null
        try {
          body = data.convertToNetty()
        } catch {
          case e: Exception =>
            // Re-encode this message as BlockFetchFailure.
            logError(s"Error opening block $blockId for client ${ctx.channel.remoteAddress}", e)
            encode(ctx, new BlockFetchFailure(blockId, e.getMessage), out)
            return
        }

        // If we got here, body cannot be null
        // 8 bytes = long for frame length
        // 1 byte = message id (type)
        // 1 byte = block id length
        // followed by block id itself
        val headerLength = 8 + 1 + 1 + blockId.length
        val frameLength = headerLength + data.size
        val header = ctx.alloc().buffer(headerLength)
        header.writeLong(frameLength)
        header.writeByte(in.id)
        ProtocolUtils.writeBlockId(header, blockId)

        assert(header.writableBytes() == 0)
        out.add(header)
        out.add(body)

      case BlockFetchFailure(blockId, error) =>
        val frameLength = 8 + 1 + 1 + blockId.length + error.length
        val buf = ctx.alloc().buffer(frameLength)
        buf.writeLong(frameLength)
        buf.writeByte(in.id)
        ProtocolUtils.writeBlockId(buf, blockId)
        buf.writeBytes(error.getBytes)

        assert(buf.writableBytes() == 0)
        out.add(buf)
    }
  }
}


/**
 * Decoder in the client side to decode server responses.
 * This decoder is stateless so it is safe to be shared by multiple threads.
 *
 * This assumes the inbound messages have been processed by a frame decoder created by
 * [[ProtocolUtils.createFrameDecoder()]].
 */
@Sharable
private[netty]
final class ServerResponseDecoder extends MessageToMessageDecoder[ByteBuf] {
  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: JList[AnyRef]): Unit = {
    val msgId = in.readByte()
    val decoded = msgId match {
      case 0 =>  // BlockFetchSuccess
        val blockId = ProtocolUtils.readBlockId(in)
        in.retain()
        new BlockFetchSuccess(blockId, new NettyManagedBuffer(in))

      case 1 =>  // BlockFetchFailure
        val blockId = ProtocolUtils.readBlockId(in)
        val errorBytes = new Array[Byte](in.readableBytes())
        in.readBytes(errorBytes)
        new BlockFetchFailure(blockId, new String(errorBytes))
    }

    assert(decoded.id == msgId)
    out.add(decoded)
  }
}


private[netty] object ProtocolUtils {

  /** LengthFieldBasedFrameDecoder used before all decoders. */
  def createFrameDecoder(): ByteToMessageDecoder = {
    // maxFrameLength = 2G
    // lengthFieldOffset = 0
    // lengthFieldLength = 8
    // lengthAdjustment = -8, i.e. exclude the 8 byte length itself
    // initialBytesToStrip = 8, i.e. strip out the length field itself
    new LengthFieldBasedFrameDecoder(Int.MaxValue, 0, 8, -8, 8)
  }

  // TODO(rxin): Make sure these work for all charsets.
  def readBlockId(in: ByteBuf): String = {
    val numBytesToRead = in.readByte().toInt
    val bytes = new Array[Byte](numBytesToRead)
    in.readBytes(bytes)
    new String(bytes)
  }

  def writeBlockId(out: ByteBuf, blockId: String): Unit = {
    out.writeByte(blockId.length)
    out.writeBytes(blockId.getBytes)
  }
}
