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

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

/** Request to get the cause of a corrupted block. Returns {@link CorruptionCause} */
public class DiagnoseCorruption extends BlockTransferMessage {
    private final String appId;
    private final String execId;
    public final String blockId;
    public final long checksum;

    public DiagnoseCorruption(String appId, String execId, String blockId, long checksum) {
      this.appId = appId;
      this.execId = execId;
      this.blockId = blockId;
      this.checksum = checksum;
    }

    @Override
    protected Type type() {
        return Type.DIAGNOSE_CORRUPTION;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("appId", appId)
        .append("execId", execId)
        .append("blockId", blockId)
        .append("checksum", checksum)
        .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiagnoseCorruption that = (DiagnoseCorruption) o;

        if (!appId.equals(that.appId)) return false;
        if (!execId.equals(that.execId)) return false;
        if (!blockId.equals(that.blockId)) return false;
        return checksum == that.checksum;
    }

    @Override
    public int hashCode() {
        int result = appId.hashCode();
        result = 31 * result + execId.hashCode();
        result = 31 * result + blockId.hashCode();
        result = 31 * result + (int) checksum;
        return result;
    }

    @Override
    public int encodedLength() {
      return Encoders.Strings.encodedLength(appId)
        + Encoders.Strings.encodedLength(execId)
        + Encoders.Strings.encodedLength(blockId)
        + 8; /* encoded length of checksum */
    }

    @Override
    public void encode(ByteBuf buf) {
      Encoders.Strings.encode(buf, appId);
      Encoders.Strings.encode(buf, execId);
      Encoders.Strings.encode(buf, blockId);
      buf.writeLong(checksum);
    }

    public static DiagnoseCorruption decode(ByteBuf buf) {
      String appId = Encoders.Strings.decode(buf);
      String execId = Encoders.Strings.decode(buf);
      String blockId = Encoders.Strings.decode(buf);
      long checksum = buf.readLong();
      return new DiagnoseCorruption(appId, execId, blockId, checksum);
    }
}
