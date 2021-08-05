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

package org.apache.spark.network.server;

import java.nio.ByteBuffer;

/**
 * A special RuntimeException thrown when shuffle service experiences a non-fatal failure
 * with handling block push requests with push-based shuffle. Due to the best-effort nature
 * of push-based shuffle, there are cases where the exceptions gets thrown under certain
 * relatively common cases such as when a pushed block is received after the corresponding
 * shuffle is merge finalized or when a pushed block experiences merge collision. Under these
 * scenarios, we throw this special RuntimeException.
 */
public class BlockPushNonFatalFailure extends RuntimeException {

  /**
   * The error code of the failure, encoded as a ByteBuffer to be responded back to the client.
   * Instead of responding a RPCFailure with the exception stack trace as the payload,
   * which makes checking the content of the exception very tedious on the client side,
   * we can respond a proper RPCResponse to make it more robust and efficient. This
   * field is only set on the shuffle server side when the exception is originally generated.
   */
  private ByteBuffer response;

  /**
   * The error code of the failure. This field is only set on the client side when a
   * BlockPushNonFatalFailure is recreated from the error code received from the server.
   */
  private ReturnCode returnCode;

  public BlockPushNonFatalFailure(ByteBuffer response) {
    this.response = response;
  }

  public BlockPushNonFatalFailure(ReturnCode returnCode) {
    this.returnCode = returnCode;
  }

  /**
   * Since this type of exception is used to only convey the error code, we reduce the
   * exception initialization overhead by skipping filling the stack trace.
   */
  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }

  public ByteBuffer getResponse() {
    return response;
  }

  public ReturnCode getReturnCode() {
    return returnCode;
  }

  public enum ReturnCode {
    /**
     * Indicate the case of a successful merge of a pushed block.
     */
    SUCCESS(0),
    /**
     * Indicate a block to be merged arrives too late on the server side, i.e. after the
     * corresponding shuffle has been merge finalized. When the client gets this code, it
     * will not retry pushing the block.
     */
    TOO_LATE_BLOCK_PUSH(1),
    /**
     * Indicating the server couldn't append a block after all available attempts due to
     * collision with other blocks belonging to the same shuffle partition.
     */
    BLOCK_APPEND_COLLISION_DETECTED(2),
    /**
     * Indicate a block received on the server side is a stale block push in the case of
     * indeterminate stage retries. When the client receives this code, it will not retry
     * pushing the block.
     */
    STALE_BLOCK_PUSH(3);

    private final byte id;

    ReturnCode(int id) {
      assert id < 128 : "Cannot have more than 128 block push return code";
      this.id = (byte) id;
    }

    public byte id() { return id; }
  }

  public static ReturnCode getReturnCode(byte id) {
    switch (id) {
      case 0: return ReturnCode.SUCCESS;
      case 1: return ReturnCode.TOO_LATE_BLOCK_PUSH;
      case 2: return ReturnCode.BLOCK_APPEND_COLLISION_DETECTED;
      case 3: return ReturnCode.STALE_BLOCK_PUSH;
      default: throw new IllegalArgumentException("Unknown block push return code: " + id);
    }
  }
}
