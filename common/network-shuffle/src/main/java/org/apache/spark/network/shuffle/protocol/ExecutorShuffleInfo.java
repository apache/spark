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

package org.apache.spark.network.shuffle.protocol;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

/** Contains all configuration necessary for locating the shuffle files of an executor. */
public class ExecutorShuffleInfo implements Encodable {
  /** The base set of local directories that the executor stores its shuffle files in. */
  public final String[] localDirs;
  /** Number of subdirectories created within each localDir. */
  public final int subDirsPerLocalDir;
  /** Shuffle manager (SortShuffleManager) that the executor is using. */
  public final String shuffleManager;

  @JsonCreator
  public ExecutorShuffleInfo(
      @JsonProperty("localDirs") String[] localDirs,
      @JsonProperty("subDirsPerLocalDir") int subDirsPerLocalDir,
      @JsonProperty("shuffleManager") String shuffleManager) {
    this.localDirs = localDirs;
    this.subDirsPerLocalDir = subDirsPerLocalDir;
    this.shuffleManager = shuffleManager;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(subDirsPerLocalDir, shuffleManager) * 41 + Arrays.hashCode(localDirs);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("localDirs", Arrays.toString(localDirs))
      .add("subDirsPerLocalDir", subDirsPerLocalDir)
      .add("shuffleManager", shuffleManager)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof ExecutorShuffleInfo) {
      ExecutorShuffleInfo o = (ExecutorShuffleInfo) other;
      return Arrays.equals(localDirs, o.localDirs)
        && Objects.equal(subDirsPerLocalDir, o.subDirsPerLocalDir)
        && Objects.equal(shuffleManager, o.shuffleManager);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.StringArrays.encodedLength(localDirs)
        + 4 // int
        + Encoders.Strings.encodedLength(shuffleManager);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.StringArrays.encode(buf, localDirs);
    buf.writeInt(subDirsPerLocalDir);
    Encoders.Strings.encode(buf, shuffleManager);
  }

  public static ExecutorShuffleInfo decode(ByteBuf buf) {
    String[] localDirs = Encoders.StringArrays.decode(buf);
    int subDirsPerLocalDir = buf.readInt();
    String shuffleManager = Encoders.Strings.decode(buf);
    return new ExecutorShuffleInfo(localDirs, subDirsPerLocalDir, shuffleManager);
  }
}
