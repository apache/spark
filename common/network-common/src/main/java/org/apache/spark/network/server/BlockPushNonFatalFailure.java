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

import com.google.common.base.Preconditions;

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
   * String constant used for generating exception messages indicating a block to be merged
   * arrives too late on the server side. When we get a block push failure because of the
   * block arrives too late, we will not retry pushing the block nor log the exception on
   * the client side.
   */
  public static final String TOO_LATE_BLOCK_PUSH_MESSAGE_SUFFIX =
    " is received after merged shuffle is finalized";

  /**
   * String constant used for generating exception messages indicating the application attempt is
   * not the latest attempt on the server side. When we get a block push failure because of the too
   * old attempt, we will not retry pushing the block nor log the exception on the client side.
   */
  public static final String TOO_OLD_ATTEMPT_SUFFIX =
    " is from an older app attempt";

  /**
   * String constant used for generating exception messages indicating a block to be merged
   * is a stale block push in the case of indeterminate stage retries on the server side.
   * When we get a block push failure because of the block push being stale, we will not
   * retry pushing the block nor log the exception on the client side.
   */
  public static final String STALE_BLOCK_PUSH_MESSAGE_SUFFIX =
    " is a stale block push from an indeterminate stage retry";

  /**
   * String constant used for generating exception messages indicating the server couldn't
   * append a block after all available attempts due to collision with other blocks belonging
   * to the same shuffle partition. When we get a block push failure because of the block
   * couldn't be written due to this reason, we will not log the exception on the client side.
   */
  public static final String BLOCK_APPEND_COLLISION_MSG_SUFFIX =
    " experienced merge collision on the server side";

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

  public BlockPushNonFatalFailure(ByteBuffer response, String msg) {
    super(msg);
    this.response = response;
  }

  public BlockPushNonFatalFailure(ReturnCode returnCode, String msg) {
    super(msg);
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
    // Ensure we do not invoke this method if response is not set
    Preconditions.checkNotNull(response);
    return response;
  }

  public ReturnCode getReturnCode() {
    // Ensure we do not invoke this method if returnCode is not set
    Preconditions.checkNotNull(returnCode);
    return returnCode;
  }

  public enum ReturnCode {
    /**
     * Indicate the case of a successful merge of a pushed block.
     */
    SUCCESS(0, ""),
    /**
     * Indicate a block to be merged arrives too late on the server side, i.e. after the
     * corresponding shuffle has been merge finalized. When the client gets this code, it
     * will not retry pushing the block.
     */
    TOO_LATE_BLOCK_PUSH(1, TOO_LATE_BLOCK_PUSH_MESSAGE_SUFFIX),
    /**
     * Indicating the server couldn't append a block after all available attempts due to
     * collision with other blocks belonging to the same shuffle partition.
     */
    BLOCK_APPEND_COLLISION_DETECTED(2, BLOCK_APPEND_COLLISION_MSG_SUFFIX),
    /**
     * Indicate a block received on the server side is a stale block push in the case of
     * indeterminate stage retries. When the client receives this code, it will not retry
     * pushing the block.
     */
    STALE_BLOCK_PUSH(3, STALE_BLOCK_PUSH_MESSAGE_SUFFIX),
    /**
     * Indicate the application attempt is not the latest attempt on the server side.
     * When the client gets this code, it will not retry pushing the block.
     */
    TOO_OLD_ATTEMPT_PUSH(4, TOO_OLD_ATTEMPT_SUFFIX);

    private final byte id;
    // Error message suffix used to generate an error message for a given ReturnCode and
    // a given block ID
    private final String errorMsgSuffix;

    ReturnCode(int id, String errorMsgSuffix) {
      assert id < 128 : "Cannot have more than 128 block push return code";
      this.id = (byte) id;
      this.errorMsgSuffix = errorMsgSuffix;
    }

    public byte id() { return id; }
  }

  public static ReturnCode getReturnCode(byte id) {
    switch (id) {
      case 0: return ReturnCode.SUCCESS;
      case 1: return ReturnCode.TOO_LATE_BLOCK_PUSH;
      case 2: return ReturnCode.BLOCK_APPEND_COLLISION_DETECTED;
      case 3: return ReturnCode.STALE_BLOCK_PUSH;
      case 4: return ReturnCode.TOO_OLD_ATTEMPT_PUSH;
      default: throw new IllegalArgumentException("Unknown block push return code: " + id);
    }
  }

  public static boolean shouldNotRetryErrorCode(ReturnCode returnCode) {
    return returnCode == ReturnCode.TOO_LATE_BLOCK_PUSH ||
      returnCode == ReturnCode.STALE_BLOCK_PUSH ||
      returnCode == ReturnCode.TOO_OLD_ATTEMPT_PUSH;
  }

  public static String getErrorMsg(String blockId, ReturnCode errorCode) {
    Preconditions.checkArgument(errorCode != ReturnCode.SUCCESS);
    return "Block " + blockId + errorCode.errorMsgSuffix;
  }
}
