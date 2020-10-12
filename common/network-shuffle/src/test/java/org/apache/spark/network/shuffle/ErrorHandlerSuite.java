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

import static org.junit.Assert.*;

/**
 * Test suite for {@link ErrorHandler}
 */
public class ErrorHandlerSuite {

  @Test
  public void testPushErrorRetry() {
    ErrorHandler.BlockPushErrorHandler handler = new ErrorHandler.BlockPushErrorHandler();
    assertFalse(handler.shouldRetryError(new RuntimeException(new IllegalArgumentException(
      ErrorHandler.BlockPushErrorHandler.TOO_LATE_MESSAGE_SUFFIX))));
    assertFalse(handler.shouldRetryError(new RuntimeException(new ConnectException())));
    assertTrue(handler.shouldRetryError(new RuntimeException(new IllegalArgumentException(
      ErrorHandler.BlockPushErrorHandler.BLOCK_APPEND_COLLISION_DETECTED_MSG_PREFIX))));
    assertTrue(handler.shouldRetryError(new Throwable()));
  }

  @Test
  public void testPushErrorLogging() {
    ErrorHandler.BlockPushErrorHandler handler = new ErrorHandler.BlockPushErrorHandler();
    assertFalse(handler.shouldLogError(new RuntimeException(new IllegalArgumentException(
      ErrorHandler.BlockPushErrorHandler.TOO_LATE_MESSAGE_SUFFIX))));
    assertFalse(handler.shouldLogError(new RuntimeException(new IllegalArgumentException(
      ErrorHandler.BlockPushErrorHandler.BLOCK_APPEND_COLLISION_DETECTED_MSG_PREFIX))));
    assertTrue(handler.shouldLogError(new Throwable()));
  }
}
