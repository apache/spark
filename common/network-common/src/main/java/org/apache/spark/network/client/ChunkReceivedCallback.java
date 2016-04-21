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

/**
 * Callback for the result of a single chunk result. For a single stream, the callbacks are
 * guaranteed to be called by the same thread in the same order as the requests for chunks were
 * made.
 *
 * Note that if a general stream failure occurs, all outstanding chunk requests may be failed.
 */
public interface ChunkReceivedCallback {
  /**
   * Called upon receipt of a particular chunk.
   *
   * The given buffer will initially have a refcount of 1, but will be release()'d as soon as this
   * call returns. You must therefore either retain() the buffer or copy its contents before
   * returning.
   */
  void onSuccess(int chunkIndex, ManagedBuffer buffer);

  /**
   * Called upon failure to fetch a particular chunk. Note that this may actually be called due
   * to failure to fetch a prior chunk in this stream.
   *
   * After receiving a failure, the stream may or may not be valid. The client should not assume
   * that the server's side of the stream has been closed.
   */
  void onFailure(int chunkIndex, Throwable e);
}
