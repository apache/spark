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

package org.apache.spark.sql.streaming

import java.time.Duration

/**
 * TTL Configuration for state variable. State values will not be returned past ttlDuration, and
 * will be eventually removed from the state store. Any state update resets the ttl to current
 * processing time plus ttlDuration.
 *
 * Passing a TTL duration of zero will disable the TTL for the state variable. Users can also use
 * the helper method `TTLConfig.NONE` in Scala or `TTLConfig.NONE()` in Java to disable TTL for
 * the state variable.
 *
 * @param ttlDuration
 *   time to live duration for state stored in the state variable.
 */
case class TTLConfig(ttlDuration: Duration)

object TTLConfig {

  /**
   * Helper method to create a TTLConfig with expiry duration as Zero
   * @return
   *   \- TTLConfig with expiry duration as Zero
   */
  def NONE: TTLConfig = TTLConfig(Duration.ZERO)

}
