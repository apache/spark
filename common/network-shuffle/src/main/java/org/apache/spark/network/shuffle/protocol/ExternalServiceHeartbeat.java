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
import org.apache.spark.network.protocol.Encoders;

/**
 * A heartbeat sent from the driver to some ExternalService
 */
public class ExternalServiceHeartbeat extends BlockTransferMessage {
    private final String appId;

    public ExternalServiceHeartbeat(String appId) {
        this.appId = appId;
    }

    public String getAppId() { return appId; }

    @Override
    protected Type type() { return Type.HEARTBEAT; }

    @Override
    public int encodedLength() { return Encoders.Strings.encodedLength(appId); }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
    }

    public static ExternalServiceHeartbeat decode(ByteBuf buf) {
        return new ExternalServiceHeartbeat(Encoders.Strings.decode(buf));
    }
}