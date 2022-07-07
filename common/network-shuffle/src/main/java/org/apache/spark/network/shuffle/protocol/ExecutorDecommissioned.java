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
import java.util.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

/**
 * Message sent from the driver to the external shuffle service when the a executor on the node
 * has decommissioned all it's shuffle data to active executors.
 */
public class ExecutorDecommissioned extends BlockTransferMessage {
    public final String appId;
    public final String execId;
    public ExecutorDecommissioned(String appId, String execId) {
        this.appId = appId;
        this.execId = execId;
    }
    @Override
    protected Type type() { return Type.EXECUTOR_SHUFFLE_DECOMMISSIONED; }
    @Override
    public int hashCode() {
        return Objects.hash(appId, execId);
    }
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("appId", appId)
                .append("execId", execId)
                .toString();
    }
    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof ExecutorDecommissioned) {
            ExecutorDecommissioned o = (ExecutorDecommissioned) other;
            return java.util.Objects.equals(appId, o.appId)
                    && java.util.Objects.equals(execId, o.execId);
        }
        return false;
    }
    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                + Encoders.Strings.encodedLength(execId);
    }
    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
    }
    public static ExecutorDecommissioned decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        return new ExecutorDecommissioned(appId, execId);
    }
}

