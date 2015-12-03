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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Callback for streaming data. Stream data will be offered to the {@link onData(String, ByteBuffer)}
 * method as it arrives. Once all the stream data is received, {@link onComplete(String)} will be
 * called.
 * <p>
 * The network library guarantees that a single thread will call these methods at a time, but
 * different call may be made by different threads.
 */
public interface StreamCallback {
  /** Called upon receipt of stream data. */
  void onData(String streamId, ByteBuffer buf) throws IOException;

  /** Called when all data from the stream has been received. */
  void onComplete(String streamId) throws IOException;

  /** Called if there's an error reading data from the stream. */
  void onFailure(String streamId, Throwable cause) throws IOException;
}
