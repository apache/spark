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
package org.apache.spark.network.util.ssl;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 *
 */
public class NettySslEncryptionHandler implements SslEncryptionHandler {

  private final Logger logger = LoggerFactory.getLogger(NettySslEncryptionHandler.class);

  private final SSLFactory sslFactory;

  /**
   * @param sslFactory
   */
  public NettySslEncryptionHandler(SSLFactory sslFactory) {
    if (sslFactory == null)
      throw new IllegalArgumentException("SSLFactory cannot be null");

    this.sslFactory = sslFactory;
  }

  @Override
  public String getName() {
    return "SslHandler";
  }

  /**
   * Inserts a Netty {@link SslHandler} at the first position of the specified {@link ChannelPipeline}.
   * If this instance contains a "Server" {@link SSLFactory} then a Netty {@link ChunkedWriteHandler}
   * will also be inserted into the pipeline.
   *
   * @param pipeline
   */
  @Override
  public void addToPipeline(ChannelPipeline pipeline, boolean isClient) {
    pipeline.addFirst(getName(), createChannelHandler(isClient));

    // Cannot use zero-copy with HTTPS, so we add in our ChunkedWriteHandler just before our MessageEncoder
    pipeline.addBefore("encoder", "chunkedWriter", new ChunkedWriteHandler());
  }

  /**
   * Returns a new Netty {@link SslHandler}
   *
   * @param isClient
   * @return
   */
  @Override
  public ChannelHandler createChannelHandler(boolean isClient) {
    try {
      return new SslHandler(sslFactory.createSSLEngine(isClient));
    } catch (Exception e) {
      throw new RuntimeException("Error creating Netty SslHandler", e);
    }
  }

  /**
   * Wait for our SSL Handshake to complete...
   *
   * @param channelFuture
   */
  @Override
  public void onConnect(
    final ChannelFuture channelFuture, final InetSocketAddress address, long connectionTimeoutMs) {
    final SslHandler sslHandler = channelFuture.channel().pipeline().get(SslHandler.class);
    Future<Channel> future = sslHandler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
      @Override
      public void operationComplete(final Future<Channel> handshakeFuture) {
        if (handshakeFuture.isSuccess()) {
          logger.debug("{} successfully completed TLS handshake to " + address);
        } else {
          logger.debug("{} failed to complete TLS handshake to " + address, handshakeFuture.cause());
          channelFuture.channel().close();
        }
      }
    });
    future.awaitUninterruptibly(connectionTimeoutMs);
  }

  /**
   *
   */
  @Override
  public void close() {
    sslFactory.destroy();
  }

  /**
   * @return
   */
  @Override
  public boolean isEnabled() {
    return true;
  }
}
