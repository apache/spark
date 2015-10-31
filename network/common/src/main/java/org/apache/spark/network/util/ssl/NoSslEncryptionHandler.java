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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;

import java.net.InetSocketAddress;

/**
 * No op implementation of the {@link SslEncryptionHandler} interface
 */
public class NoSslEncryptionHandler implements SslEncryptionHandler {

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void addToPipeline(ChannelPipeline pipeline, boolean isClient) {
    // No op
  }

  @Override
  public ChannelHandler createChannelHandler(boolean isClient) {
    return null;
  }

  @Override
  public void onConnect(ChannelFuture channelFuture, InetSocketAddress address, long connectionTimeoutMs) {
    // No Op
  }

  @Override
  public void close() {
    // No op
  }

  @Override
  public boolean isEnabled() {
    return false;
  }
}
