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

import java.nio.ByteBuffer;

/**
 * Callback for the result of a single RPC. This will be invoked once with either success or
 * failure.
 */
public interface RpcResponseCallback extends BaseResponseCallback {
  /**
   * Successful serialized result from server.
   *
   * After `onSuccess` returns, `response` will be recycled and its content will become invalid.
   * Please copy the content of `response` if you want to use it after `onSuccess` returns.
   */
  void onSuccess(ByteBuffer response);
}
