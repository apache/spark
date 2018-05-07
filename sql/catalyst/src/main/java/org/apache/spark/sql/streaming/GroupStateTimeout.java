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

import org.apache.spark.annotation.Experimental;
import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.catalyst.plans.logical.*;

/**
 * Represents the type of timeouts possible for the Dataset operations
 * `mapGroupsWithState` and `flatMapGroupsWithState`. See documentation on
 * `GroupState` for more details.
 *
 * @since 2.2.0
 */
@Experimental
@InterfaceStability.Evolving
public class GroupStateTimeout {

  /**
   * Timeout based on processing time. The duration of timeout can be set for each group in
   * `map/flatMapGroupsWithState` by calling `GroupState.setTimeoutDuration()`. See documentation
   * on `GroupState` for more details.
   */
  public static GroupStateTimeout ProcessingTimeTimeout() {
    return ProcessingTimeTimeout$.MODULE$;
  }

  /**
   * Timeout based on event-time. The event-time timestamp for timeout can be set for each
   * group in `map/flatMapGroupsWithState` by calling `GroupState.setTimeoutTimestamp()`.
   * In addition, you have to define the watermark in the query using `Dataset.withWatermark`.
   * When the watermark advances beyond the set timestamp of a group and the group has not
   * received any data, then the group times out. See documentation on
   * `GroupState` for more details.
   */
  public static GroupStateTimeout EventTimeTimeout() { return EventTimeTimeout$.MODULE$; }

  /** No timeout. */
  public static GroupStateTimeout NoTimeout() { return NoTimeout$.MODULE$; }

}
