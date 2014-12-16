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

package org.apache.spark.network.nio

import java.nio.ByteBuffer

import org.apache.spark.storage.{BlockId, StorageLevel, TestBlockId}

import scala.collection.mutable.{ArrayBuffer, StringBuilder}

// private[spark] because we need to register them in Kryo
private[spark] case class GetBlock(id: BlockId)
private[spark] case class GotBlock(id: BlockId, data: ByteBuffer)
private[spark] case class PutBlock(id: BlockId, data: ByteBuffer, level: StorageLevel)

private[nio] class BlockMessage() {
  // Un-initialized: typ = 0
  // GetBlock: typ = 1
  // GotBlock: typ = 2
  // PutBlock: typ = 3
  private var typ: Int = BlockMessage.TYPE_NON_INITIALIZED
  private var id: BlockId = null
  private var data: ByteBuffer = null
  private var level: StorageLevel = null

  def set(getBlock: GetBlock) {
    typ = BlockMessage.TYPE_GET_BLOCK
    id = getBlock.id
  }

  def set(gotBlock: GotBlock) {
    typ = BlockMessage.TYPE_GOT_BLOCK
    id = gotBlock.id
    data = gotBlock.data
  }

  def set(putBlock: PutBlock) {
    typ = BlockMessage.TYPE_PUT_BLOCK
    id = putBlock.id
    data = putBlock.data
    level = putBlock.level
  }

  def set(buffer: ByteBuffer) {
    /*
    println()
    println("BlockMessage: ")
    while(buffer.remaining > 0) {
      print(buffer.get())
    }
    buffer.rewind()
    println()
    println()
    */
    typ = buffer.getInt()
    val idLength = buffer.getInt()
    val idBuilder = new StringBuilder(idLength)
    for (i <- 1 to idLength) {
      idBuilder += buffer.getChar()
    }
    id = BlockId(idBuilder.toString)

    if (typ == BlockMessage.TYPE_PUT_BLOCK) {

      val booleanInt = buffer.getInt()
      val replication = buffer.getInt()
      level = StorageLevel(booleanInt, replication)

      val dataLength = buffer.getInt()
      data = ByteBuffer.allocate(dataLength)
      if (dataLength != buffer.remaining) {
        throw new Exception("Error parsing buffer")
      }
      data.put(buffer)
      data.flip()
    } else if (typ == BlockMessage.TYPE_GOT_BLOCK) {

      val dataLength = buffer.getInt()
      data = ByteBuffer.allocate(dataLength)
      if (dataLength != buffer.remaining) {
        throw new Exception("Error parsing buffer")
      }
      data.put(buffer)
      data.flip()
    }

  }

  def set(bufferMsg: BufferMessage) {
    val buffer = bufferMsg.buffers.apply(0)
    buffer.clear()
    set(buffer)
  }

  def getType: Int = typ
  def getId: BlockId = id
  def getData: ByteBuffer = data
  def getLevel: StorageLevel =  level

  def toBufferMessage: BufferMessage = {
    val buffers = new ArrayBuffer[ByteBuffer]()
    var buffer = ByteBuffer.allocate(4 + 4 + id.name.length * 2)
    buffer.putInt(typ).putInt(id.name.length)
    id.name.foreach((x: Char) => buffer.putChar(x))
    buffer.flip()
    buffers += buffer

    if (typ == BlockMessage.TYPE_PUT_BLOCK) {
      buffer = ByteBuffer.allocate(8).putInt(level.toInt).putInt(level.replication)
      buffer.flip()
      buffers += buffer

      buffer = ByteBuffer.allocate(4).putInt(data.remaining)
      buffer.flip()
      buffers += buffer

      buffers += data
    } else if (typ == BlockMessage.TYPE_GOT_BLOCK) {
      buffer = ByteBuffer.allocate(4).putInt(data.remaining)
      buffer.flip()
      buffers += buffer

      buffers += data
    }

    /*
    println()
    println("BlockMessage: ")
    buffers.foreach(b => {
      while(b.remaining > 0) {
        print(b.get())
      }
      b.rewind()
    })
    println()
    println()
    */
    Message.createBufferMessage(buffers)
  }

  override def toString: String = {
    "BlockMessage [type = " + typ + ", id = " + id + ", level = " + level +
    ", data = " + (if (data != null) data.remaining.toString  else "null") + "]"
  }
}

private[nio] object BlockMessage {
  val TYPE_NON_INITIALIZED: Int = 0
  val TYPE_GET_BLOCK: Int = 1
  val TYPE_GOT_BLOCK: Int = 2
  val TYPE_PUT_BLOCK: Int = 3

  def fromBufferMessage(bufferMessage: BufferMessage): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(bufferMessage)
    newBlockMessage
  }

  def fromByteBuffer(buffer: ByteBuffer): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(buffer)
    newBlockMessage
  }

  def fromGetBlock(getBlock: GetBlock): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(getBlock)
    newBlockMessage
  }

  def fromGotBlock(gotBlock: GotBlock): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(gotBlock)
    newBlockMessage
  }

  def fromPutBlock(putBlock: PutBlock): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(putBlock)
    newBlockMessage
  }
}
