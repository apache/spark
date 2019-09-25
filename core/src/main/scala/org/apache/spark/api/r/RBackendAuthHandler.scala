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

package org.apache.spark.api.r

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets.UTF_8

import io.netty.channel.{Channel, ChannelHandlerContext, SimpleChannelInboundHandler}

import org.apache.spark.internal.Logging

/**
 * Authentication handler for connections from the R process.
 */
private class RBackendAuthHandler(secret: String)
  extends SimpleChannelInboundHandler[Array[Byte]] with Logging {

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    // The R code adds a null terminator to serialized strings, so ignore it here.
    val clientSecret = new String(msg, 0, msg.length - 1, UTF_8)
    try {
      require(secret == clientSecret, "Auth secret mismatch.")
      ctx.pipeline().remove(this)
      writeReply("ok", ctx.channel())
    } catch {
      case e: Exception =>
        logInfo("Authentication failure.", e)
        writeReply("err", ctx.channel())
        ctx.close()
    }
  }

  private def writeReply(reply: String, chan: Channel): Unit = {
    val out = new ByteArrayOutputStream()
    SerDe.writeString(new DataOutputStream(out), reply)
    chan.writeAndFlush(out.toByteArray())
  }

}
