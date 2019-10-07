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

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;

import java.util.*;
import java.util.Map.Entry;

// Needed by ScalaDoc. See SPARK-7726


/** The reply to get local dirs giving back the dirs for each of the requested executors. */
public class LocalDirsForExecutors extends BlockTransferMessage {
  private final String[] execIds;
  private final int[] numLocalDirsByExec;
  private final String[] allLocalDirs;

  public LocalDirsForExecutors(Map<String, String[]> localDirsByExec) {
    this.execIds = new String[localDirsByExec.size()];
    this.numLocalDirsByExec = new int[localDirsByExec.size()];
    ArrayList<String> localDirs = new ArrayList<>();
    int index = 0;
    for (Entry<String, String[]> e: localDirsByExec.entrySet()) {
      execIds[index] = e.getKey();
      numLocalDirsByExec[index] = e.getValue().length;
      Collections.addAll(localDirs, e.getValue());
      index++;
    }
    this.allLocalDirs = localDirs.toArray(new String[0]);
  }

  private LocalDirsForExecutors(String[] execIds, int[] numLocalDirsByExec, String[] allLocalDirs) {
    this.execIds = execIds;
    this.numLocalDirsByExec = numLocalDirsByExec;
    this.allLocalDirs = allLocalDirs;
  }

  @Override
  protected Type type() { return Type.LOCAL_DIRS_FOR_EXECUTORS; }

  @Override
  public int hashCode() {
    return Arrays.hashCode(execIds);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("execIds", Arrays.toString(execIds))
      .add("numLocalDirsByExec", Arrays.toString(numLocalDirsByExec))
      .add("allLocalDirs", Arrays.toString(allLocalDirs))
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof LocalDirsForExecutors) {
      LocalDirsForExecutors o = (LocalDirsForExecutors) other;
      return Arrays.equals(execIds, o.execIds)
        && Arrays.equals(numLocalDirsByExec, o.numLocalDirsByExec)
        && Arrays.equals(allLocalDirs, o.allLocalDirs);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.StringArrays.encodedLength(execIds)
      + Encoders.IntArrays.encodedLength(numLocalDirsByExec)
      + Encoders.StringArrays.encodedLength(allLocalDirs);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.StringArrays.encode(buf, execIds);
    Encoders.IntArrays.encode(buf, numLocalDirsByExec);
    Encoders.StringArrays.encode(buf, allLocalDirs);
  }

  public static LocalDirsForExecutors decode(ByteBuf buf) {
    String[] execIds = Encoders.StringArrays.decode(buf);
    int[] numLocalDirsByExec = Encoders.IntArrays.decode(buf);
    String[] allLocalDirs = Encoders.StringArrays.decode(buf);
    return new LocalDirsForExecutors(execIds, numLocalDirsByExec, allLocalDirs);
  }

  public Map<String, String[]> getLocalDirsByExec() {
    Map<String, String[]> localDirsByExec = new HashMap<>();
    int index = 0;
    int localDirsIndex = 0;
    for (int length: numLocalDirsByExec) {
      localDirsByExec.put(execIds[index],
        Arrays.copyOfRange(allLocalDirs, localDirsIndex, localDirsIndex + length));
      localDirsIndex += length;
      index++;
    }
    return  localDirsByExec;
  }
}
