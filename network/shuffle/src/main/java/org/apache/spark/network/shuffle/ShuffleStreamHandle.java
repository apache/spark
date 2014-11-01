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

package org.apache.spark.network.shuffle;

import java.io.Serializable;
import java.util.Arrays;

import com.google.common.base.Objects;

/**
 * Identifier for a fixed number of chunks to read from a stream created by an "open blocks"
 * message. This is used by {@link OneForOneBlockFetcher}.
 */
public class ShuffleStreamHandle implements Serializable {
  public final long streamId;
  public final int numChunks;

  public ShuffleStreamHandle(long streamId, int numChunks) {
    this.streamId = streamId;
    this.numChunks = numChunks;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId, numChunks);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .add("numChunks", numChunks)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof ShuffleStreamHandle) {
      ShuffleStreamHandle o = (ShuffleStreamHandle) other;
      return Objects.equal(streamId, o.streamId)
        && Objects.equal(numChunks, o.numChunks);
    }
    return false;
  }
}
