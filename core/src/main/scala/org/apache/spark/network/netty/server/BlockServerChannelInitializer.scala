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
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.LineBasedFrameDecoder
import io.netty.handler.codec.string.StringDecoder

import org.apache.spark.storage.BlockDataProvider

/** Channel initializer that sets up the pipeline for the BlockServer. */
private[netty]
class BlockServerChannelInitializer(dataProvider: BlockDataProvider)
  extends ChannelInitializer[SocketChannel] {

  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline
      .addLast("frameDecoder", new LineBasedFrameDecoder(1024))  // max block id length 1024
      .addLast("stringDecoder", new StringDecoder(UTF_8))
      .addLast("blockHeaderEncoder", new BlockHeaderEncoder)
      .addLast("handler", new BlockServerHandler(dataProvider))
  }
}
