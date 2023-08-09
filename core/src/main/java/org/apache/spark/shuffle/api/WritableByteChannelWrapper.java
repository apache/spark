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

package org.apache.spark.shuffle.api;

import java.io.Closeable;
import java.nio.channels.WritableByteChannel;

import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * A thin wrapper around a {@link WritableByteChannel}.
 * <p>
 * This is primarily provided for the local disk shuffle implementation to provide a
 * {@link java.nio.channels.FileChannel} that keeps the channel open across partition writes.
 *
 * @since 3.0.0
 */
@Private
public interface WritableByteChannelWrapper extends Closeable {

  /**
   * The underlying channel to write bytes into.
   */
  WritableByteChannel channel();
}
