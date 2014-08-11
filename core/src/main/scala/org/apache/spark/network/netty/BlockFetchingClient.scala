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

import java.util.concurrent.TimeoutException

import io.netty.channel.ChannelFuture


/**
 * Client for fetching remote data blocks from [[BlockServer]]. Use [[BlockFetchingClientFactory]]
 * to instantiate this client.
 *
 * See [[BlockServer]] for the client/server communication protocol.
 */
private[spark]
class BlockFetchingClient(val cf: ChannelFuture, timeout: Int) {

  @throws[InterruptedException]
  @throws[TimeoutException]
  def sendRequest(blockIds: Seq[String]): Unit = {
    // It's best to limit the number of "write" calls since it needs to traverse the whole pipeline.
    // It's also best to limit the number of "flush" calls since it requires system calls.
    // Let's concatenate the string and then call writeAndFlush once.
    val sent = cf.channel().writeAndFlush(blockIds.mkString("\n") + "\n").await(timeout)
    if (!sent) {
      throw new TimeoutException(s"Time out sending request for $blockIds")
    }
  }

  def close(): Unit = {
    // TODO: What do we need to do to close the client?
  }
}
