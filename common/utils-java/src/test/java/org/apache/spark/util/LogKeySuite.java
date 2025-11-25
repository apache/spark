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

package org.apache.spark.util;

import org.junit.jupiter.api.Test;

import org.apache.spark.internal.LogKey;
import org.apache.spark.internal.LogKeys;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Test suite for {@link LogKey} interface.
 */
public class LogKeySuite {

  /**
   * Test that the $init$ method exists and can be called without throwing an exception.
   * This method is added for backward compatibility with libraries compiled against Spark 4.0
   * where LogKey was a Scala trait. Scala traits generate a $init$ method that gets called
   * during initialization.
   */
  @Test
  public void testBackwardCompatibilityInitMethod() {
    // Test with a standard LogKey
    LogKey logKey = LogKeys.EXECUTOR_ID;
    assertDoesNotThrow(() -> LogKey.$init$(logKey));

    // Test with null (should not throw)
    assertDoesNotThrow(() -> LogKey.$init$(null));

    // Test with a custom LogKey implementation
    LogKey customLogKey = CustomLogKeys.CUSTOM_LOG_KEY;
    assertDoesNotThrow(() -> LogKey.$init$(customLogKey));
  }
}

