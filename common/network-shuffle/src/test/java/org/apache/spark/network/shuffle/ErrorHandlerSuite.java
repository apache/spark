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

package org.apache.spark.network.shuffle;

import java.net.ConnectException;

import org.junit.Test;

import org.apache.spark.network.server.BlockPushNonFatalFailure;
import org.apache.spark.network.server.BlockPushNonFatalFailure.ReturnCode;

import static org.junit.Assert.*;

/**
 * Test suite for {@link ErrorHandler}
 */
public class ErrorHandlerSuite {

  @Test
  public void testErrorRetry() {
    ErrorHandler.BlockPushErrorHandler pushHandler = new ErrorHandler.BlockPushErrorHandler();
    assertFalse(pushHandler.shouldRetryError(new BlockPushNonFatalFailure(
      ReturnCode.TOO_LATE_BLOCK_PUSH, "")));
    assertFalse(pushHandler.shouldRetryError(new BlockPushNonFatalFailure(
      ReturnCode.STALE_BLOCK_PUSH, "")));
    assertFalse(pushHandler.shouldRetryError(new RuntimeException(new ConnectException())));
    assertTrue(pushHandler.shouldRetryError(new BlockPushNonFatalFailure(
      ReturnCode.BLOCK_APPEND_COLLISION_DETECTED, "")));
    assertTrue(pushHandler.shouldRetryError(new Throwable()));

    ErrorHandler.BlockFetchErrorHandler fetchHandler = new ErrorHandler.BlockFetchErrorHandler();
    assertFalse(fetchHandler.shouldRetryError(new RuntimeException(
      ErrorHandler.BlockFetchErrorHandler.STALE_SHUFFLE_BLOCK_FETCH)));
  }

  @Test
  public void testErrorLogging() {
    ErrorHandler.BlockPushErrorHandler pushHandler = new ErrorHandler.BlockPushErrorHandler();
    assertFalse(pushHandler.shouldLogError(new BlockPushNonFatalFailure(
      ReturnCode.TOO_LATE_BLOCK_PUSH, "")));
    assertFalse(pushHandler.shouldLogError(new BlockPushNonFatalFailure(
      ReturnCode.STALE_BLOCK_PUSH, "")));
    assertFalse(pushHandler.shouldLogError(new BlockPushNonFatalFailure(
      ReturnCode.BLOCK_APPEND_COLLISION_DETECTED, "")));
    assertTrue(pushHandler.shouldLogError(new Throwable()));

    ErrorHandler.BlockFetchErrorHandler fetchHandler = new ErrorHandler.BlockFetchErrorHandler();
    assertFalse(fetchHandler.shouldLogError(new RuntimeException(
      ErrorHandler.BlockFetchErrorHandler.STALE_SHUFFLE_BLOCK_FETCH)));
  }
}
