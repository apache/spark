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

import com.google.common.base.Charsets.UTF_8
import io.netty.buffer.ByteBuf
import io.netty.channel.embedded.EmbeddedChannel

import org.scalatest.FunSuite

class BlockHeaderEncoderSuite extends FunSuite {

  test("encode normal block data") {
    val blockId = "test_block"
    val channel = new EmbeddedChannel(new BlockHeaderEncoder)
    channel.writeOutbound(new BlockHeader(17, blockId, None))
    val out = channel.readOutbound().asInstanceOf[ByteBuf]
    assert(out.readInt() === 4 + blockId.length + 17)
    assert(out.readInt() === blockId.length)

    val blockIdBytes = new Array[Byte](blockId.length)
    out.readBytes(blockIdBytes)
    assert(new String(blockIdBytes, UTF_8) === blockId)
    assert(out.readableBytes() === 0)

    channel.close()
  }

  test("encode error message") {
    val blockId = "error_block"
    val errorMsg = "error encountered"
    val channel = new EmbeddedChannel(new BlockHeaderEncoder)
    channel.writeOutbound(new BlockHeader(17, blockId, Some(errorMsg)))
    val out = channel.readOutbound().asInstanceOf[ByteBuf]
    assert(out.readInt() === 4 + blockId.length + errorMsg.length)
    assert(out.readInt() === -blockId.length)

    val blockIdBytes = new Array[Byte](blockId.length)
    out.readBytes(blockIdBytes)
    assert(new String(blockIdBytes, UTF_8) === blockId)

    val errorMsgBytes = new Array[Byte](errorMsg.length)
    out.readBytes(errorMsgBytes)
    assert(new String(errorMsgBytes, UTF_8) === errorMsg)
    assert(out.readableBytes() === 0)

    channel.close()
  }
}
