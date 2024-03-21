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

package org.apache.spark.sql.streaming;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.catalyst.plans.logical.*;

/**
 * Represents the type of timeouts possible for the Dataset operations
 * {@code transformWithState}.
 */
@Experimental
@Evolving
public class TimeoutMode {
  /**
   * Stateful processor that does not register timers
   */
  public static final TimeoutMode NoTimeouts() {
    return NoTimeouts$.MODULE$;
  }

  /**
   * Stateful processor that only registers processing time based timers
   */
  public static final TimeoutMode ProcessingTime() {
    return ProcessingTime$.MODULE$;
  }

  /**
   * Stateful processor that only registers event time based timers
   */
  public static final TimeoutMode EventTime() {
    return EventTime$.MODULE$;
  }
}
