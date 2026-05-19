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

package org.apache.spark.network.shuffle.streaming;

public enum StreamingShuffleMessageType {
    DATA_MESSAGE_UNSAFE_ROW(1),
    CREDIT_CONTROL_MESSAGE(2),
    TERMINATION_CONTROL_MESSAGE(3),
    TERMINATION_ACK_MESSAGE(4);

    private final int id;

    StreamingShuffleMessageType(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static StreamingShuffleMessageType decode(int givenId) {
        return switch (givenId) {
            case 1 -> DATA_MESSAGE_UNSAFE_ROW;
            case 2 -> CREDIT_CONTROL_MESSAGE;
            case 3 -> TERMINATION_CONTROL_MESSAGE;
            case 4 -> TERMINATION_ACK_MESSAGE;
            default -> throw new IllegalArgumentException("Unknown message type: " + givenId);
        };
    }
}
