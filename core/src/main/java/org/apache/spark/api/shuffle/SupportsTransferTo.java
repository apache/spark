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

import java.io.IOException;

import org.apache.spark.annotation.Experimental;

/**
 * :: Experimental ::
 * Indicates that partition writers can transfer bytes directly from input byte channels to
 * output channels that stream data to the underlying shuffle partition storage medium.
 * <p>
 * This API is separated out for advanced users because it only needs to be used for
 * specific low-level optimizations. The idea is that the returned channel can transfer bytes
 * from the input file channel out to the backing storage system without copying data into
 * memory.
 * <p>
 * Most shuffle plugin implementations should use {@link ShufflePartitionWriter} instead.
 *
 * @since 3.0.0
 */
@Experimental
public interface SupportsTransferTo extends ShufflePartitionWriter {

  /**
   * Opens and returns a {@link TransferrableWritableByteChannel} for transferring bytes from
   * input byte channels to the underlying shuffle data store.
   */
  TransferrableWritableByteChannel openTransferrableChannel() throws IOException;

  /**
   * Returns the number of bytes written either by this writer's output stream opened by
   * {@link #openStream()} or the byte channel opened by {@link #openTransferrableChannel()}.
   */
  @Override
  long getNumBytesWritten();
}
