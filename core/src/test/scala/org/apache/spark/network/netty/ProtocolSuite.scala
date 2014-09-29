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

import io.netty.channel.embedded.EmbeddedChannel

import org.scalatest.FunSuite


/**
 * Test client/server encoder/decoder protocol.
 */
class ProtocolSuite extends FunSuite {

  /**
   * Helper to test server to client message protocol by encoding a message and decoding it.
   */
  private def testServerToClient(msg: ServerResponse) {
    val serverChannel = new EmbeddedChannel(new ServerResponseEncoder)
    serverChannel.writeOutbound(msg)

    val clientChannel = new EmbeddedChannel(
      ProtocolUtils.createFrameDecoder(),
      new ServerResponseDecoder)

    // Drain all server outbound messages and write them to the client's server decoder.
    while (!serverChannel.outboundMessages().isEmpty) {
      clientChannel.writeInbound(serverChannel.readOutbound())
    }

    assert(clientChannel.inboundMessages().size === 1)
    // Must put "msg === ..." instead of "... === msg" since only TestManagedBuffer equals is
    // overridden.
    assert(msg === clientChannel.readInbound())
  }

  /**
   * Helper to test client to server message protocol by encoding a message and decoding it.
   */
  private def testClientToServer(msg: ClientRequest) {
    val clientChannel = new EmbeddedChannel(new ClientRequestEncoder)
    clientChannel.writeOutbound(msg)

    val serverChannel = new EmbeddedChannel(
      ProtocolUtils.createFrameDecoder(),
      new ClientRequestDecoder)

    // Drain all client outbound messages and write them to the server's decoder.
    while (!clientChannel.outboundMessages().isEmpty) {
      serverChannel.writeInbound(clientChannel.readOutbound())
    }

    assert(serverChannel.inboundMessages().size === 1)
    // Must put "msg === ..." instead of "... === msg" since only TestManagedBuffer equals is
    // overridden.
    assert(msg === serverChannel.readInbound())
  }

  test("server to client protocol - BlockFetchSuccess(\"a1234\", new TestManagedBuffer(10))") {
    testServerToClient(BlockFetchSuccess("a1234", new TestManagedBuffer(10)))
  }

  test("server to client protocol - BlockFetchSuccess(\"\", new TestManagedBuffer(0))") {
    testServerToClient(BlockFetchSuccess("", new TestManagedBuffer(0)))
  }

  test("server to client protocol - BlockFetchFailure(\"abcd\", \"this is an error\")") {
    testServerToClient(BlockFetchFailure("abcd", "this is an error"))
  }

  test("server to client protocol - BlockFetchFailure(\"\", \"\")") {
    testServerToClient(BlockFetchFailure("", ""))
  }

  test("client to server protocol - BlockFetchRequest(Seq.empty[String])") {
    testClientToServer(BlockFetchRequest(Seq.empty[String]))
  }

  test("client to server protocol - BlockFetchRequest(Seq(\"b1\"))") {
    testClientToServer(BlockFetchRequest(Seq("b1")))
  }

  test("client to server protocol - BlockFetchRequest(Seq(\"b1\", \"b2\", \"b3\"))") {
    testClientToServer(BlockFetchRequest(Seq("b1", "b2", "b3")))
  }

  ignore("client to server protocol - BlockUploadRequest(\"\", new TestManagedBuffer(0))") {
    testClientToServer(BlockUploadRequest("", new TestManagedBuffer(0)))
  }

  ignore("client to server protocol - BlockUploadRequest(\"b_upload\", new TestManagedBuffer(10))") {
    testClientToServer(BlockUploadRequest("b_upload", new TestManagedBuffer(10)))
  }
}
