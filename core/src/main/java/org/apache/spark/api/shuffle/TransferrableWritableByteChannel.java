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

import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.spark.annotation.Experimental;

/**
 * :: Experimental ::
 * Represents an output byte channel that can copy bytes from input file channels to some
 * arbitrary storage system.
 * <p>
 * This API is provided for advanced users who can transfer bytes from a file channel to
 * some output sink without copying data into memory. Most users should not need to use
 * this functionality; this is primarily provided for the built-in shuffle storage backends
 * that persist shuffle files on local disk.
 * <p>
 * For a simpler alternative, see {@link ShufflePartitionWriter}.
 *
 * @since 3.0.0
 */
@Experimental
public interface TransferrableWritableByteChannel extends Closeable {

  /**
   * Copy all bytes from the source readable byte channel into this byte channel.
   *
   * @param source File to transfer bytes from. Do not call anything on this channel other than
   *               {@link FileChannel#transferTo(long, long, WritableByteChannel)}.
   * @param transferStartPosition Start position of the input file to transfer from.
   * @param numBytesToTransfer Number of bytes to transfer from the given source.
   */
  void transferFrom(FileChannel source, long transferStartPosition, long numBytesToTransfer)
      throws IOException;
}
