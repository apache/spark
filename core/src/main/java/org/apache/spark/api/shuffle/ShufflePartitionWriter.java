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

package org.apache.spark.api.shuffle;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.http.annotation.Experimental;

/**
 * :: Experimental ::
 * An interface for giving streams / channels for shuffle writes.
 *
 * @since 3.0.0
 */
@Experimental
public interface ShufflePartitionWriter extends Closeable {

  /**
   * Returns an underlying {@link OutputStream} that can write bytes to the underlying data store.
   * <p>
   * Note that this stream itself is not closed by the caller; close the stream in the
   * implementation of this interface's {@link #close()}.
   */
  OutputStream toStream() throws IOException;

  /**
   * Returns an underlying {@link WritableByteChannel} that can write bytes to the underlying data
   * store.
   * <p>
   * Note that this channel itself is not closed by the caller; close the channel in the
   * implementation of this interface's {@link #close()}.
   */
  default WritableByteChannel toChannel() throws IOException {
    return Channels.newChannel(toStream());
  }

  /**
   * Get the number of bytes written by this writer's stream returned by {@link #toStream()} or
   * the channel returned by {@link #toChannel()}.
   */
  long getNumBytesWritten();

  /**
   * Close all resources created by this ShufflePartitionWriter, via calls to {@link #toStream()}
   * or {@link #toChannel()}.
   * <p>
   * This must always close any stream returned by {@link #toStream()}.
   * <p>
   * Note that the default version of {@link #toChannel()} returns a {@link WritableByteChannel}
   * that does not itself need to be closed up front; only the underlying output stream given by
   * {@link #toStream()} must be closed.
   */
  @Override
  void close() throws IOException;
}
