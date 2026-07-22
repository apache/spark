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
package org.apache.spark.network.client;

import org.apache.spark.network.buffer.ManagedBuffer;

public interface ManagedRpcResponseCallback extends BaseResponseCallback {

  /**
   * Successful response body.
   * Ownership of {@code response} is transferred to the callback.
   * The callback implementation MUST ensure {@code response} is released exactly once:
   * either hand it off to the transport (e.g. wrap it in {@code RpcResponse} and write it to
   * the channel, letting Netty release it after write completes), or call
   * {@code response.release()} itself if it is not handed off.
   * It may call {@code retain()} if it needs to pass the buffer to another thread.
   */
  void onSuccess(ManagedBuffer response);
}
