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

package org.apache.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

/**
 * Abstract class for response messages that contain a large data portion kept in a separate
 * buffer. These messages are treated especially by MessageEncoder.
 */
public abstract class ResponseWithBody implements ResponseMessage {
  public final ManagedBuffer body;
  public final boolean isBodyInFrame;

  protected ResponseWithBody(ManagedBuffer body, boolean isBodyInFrame) {
    this.body = body;
    this.isBodyInFrame = isBodyInFrame;
  }

  public abstract ResponseMessage createFailureResponse(String error);
}
