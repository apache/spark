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
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

/** Contains all configuration necessary for locating the shuffle files of an executor. */
public class ExecutorShuffleInfo implements Encodable {
  /** The base set of local directories that the executor stores its shuffle files in. */
  public final String[] localDirs;
  /** Number of subdirectories created within each localDir. */
  public final int subDirsPerLocalDir;
  /**
   * Shuffle manager (SortShuffleManager) that the executor is using.
   * If this string contains semicolon, it will also include the meta information
   * for push based shuffle in JSON format. Example of the string with semicolon would be:
   * SortShuffleManager:{"mergeDir": "mergeDirectory_1", "attemptId": 1}
   */
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
    return Objects.hash(subDirsPerLocalDir, shuffleManager) * 41 + Arrays.hashCode(localDirs);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("localDirs", Arrays.toString(localDirs))
      .append("subDirsPerLocalDir", subDirsPerLocalDir)
      .append("shuffleManager", shuffleManager)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ExecutorShuffleInfo) {
      ExecutorShuffleInfo o = (ExecutorShuffleInfo) other;
      return Arrays.equals(localDirs, o.localDirs)
        && subDirsPerLocalDir == o.subDirsPerLocalDir
        && Objects.equals(shuffleManager, o.shuffleManager);
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
