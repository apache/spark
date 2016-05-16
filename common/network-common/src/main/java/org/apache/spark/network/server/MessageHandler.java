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

import org.apache.spark.network.protocol.Message;

/**
 * Handles either request or response messages coming off of Netty. A MessageHandler instance
 * is associated with a single Netty Channel (though it may have multiple clients on the same
 * Channel.)
 */
public abstract class MessageHandler<T extends Message> {
  /** Handles the receipt of a single message. */
  public abstract void handle(T message) throws Exception;

  /** Invoked when the channel this MessageHandler is on is active. */
  public abstract void channelActive();

  /** Invoked when an exception was caught on the Channel. */
  public abstract void exceptionCaught(Throwable cause);

  /** Invoked when the channel this MessageHandler is on is inactive. */
  public abstract void channelInactive();
}
