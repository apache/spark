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

package org.apache.spark.network.server;

import java.nio.ByteBuffer;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.client.TransportClient;

/**
 * RPC Handler which performs authentication, and when it's successful, delegates further
 * calls to another RPC handler. The authentication handshake itself should be implemented
 * by subclasses.
 */
public abstract class AbstractAuthRpcHandler extends RpcHandler {
  /** RpcHandler we will delegate to for authenticated connections. */
  private final RpcHandler delegate;

  private boolean isAuthenticated;

  protected AbstractAuthRpcHandler(RpcHandler delegate) {
    this.delegate = delegate;
  }

  /**
   * Responds to an authentication challenge.
   *
   * @return Whether the client is authenticated.
   */
  protected abstract boolean doAuthChallenge(
      TransportClient client,
      ByteBuffer message,
      RpcResponseCallback callback);

  @Override
  public final void receive(
      TransportClient client,
      ByteBuffer message,
      RpcResponseCallback callback) {
    if (isAuthenticated) {
      delegate.receive(client, message, callback);
    } else {
      isAuthenticated = doAuthChallenge(client, message, callback);
    }
  }

  @Override
  public final void receive(TransportClient client, ByteBuffer message) {
    if (isAuthenticated) {
      delegate.receive(client, message);
    } else {
      throw new SecurityException("Unauthenticated call to receive().");
    }
  }

  @Override
  public final StreamCallbackWithID receiveStream(
      TransportClient client,
      ByteBuffer message,
      RpcResponseCallback callback) {
    if (isAuthenticated) {
      return delegate.receiveStream(client, message, callback);
    } else {
      throw new SecurityException("Unauthenticated call to receiveStream().");
    }
  }

  @Override
  public StreamManager getStreamManager() {
    return delegate.getStreamManager();
  }

  @Override
  public void channelActive(TransportClient client) {
    delegate.channelActive(client);
  }

  @Override
  public void channelInactive(TransportClient client) {
    delegate.channelInactive(client);
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    delegate.exceptionCaught(cause, client);
  }

  public boolean isAuthenticated() {
    return isAuthenticated;
  }

  @Override
  public MergedBlockMetaReqHandler getMergedBlockMetaReqHandler() {
    return delegate.getMergedBlockMetaReqHandler();
  }
}
