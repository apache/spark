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

package org.apache.spark.sql.connector.read.streaming;

import org.apache.spark.annotation.Evolving;

/**
 * An interface for streaming sources that supports running in Trigger.AvailableNow mode, which
 * will process all the available data at the beginning of the query in (possibly) multiple batches.
 *
 * This mode will have better scalability comparing to Trigger.Once mode.
 *
 * @since 3.3.0
 */
@Evolving
public interface SupportsTriggerAvailableNow extends SupportsAdmissionControl {

  /**
   * This will be called at the beginning of streaming queries with Trigger.AvailableNow, to let the
   * source record the offset for the current latest data at the time (a.k.a the target offset for
   * the query). The source will behave as if there is no new data coming in after the target
   * offset, i.e., the source will not return an offset higher than the target offset when
   * {@link #latestOffset(Offset, ReadLimit) latestOffset} is called.
   * <p>
   * Note that there is an exception on the first uncommitted batch after a restart, where the end
   * offset is not derived from the current latest offset. Sources need to take special
   * considerations if wanting to assert such relation. One possible way is to have an internal
   * flag in the source to indicate whether it is Trigger.AvailableNow, set the flag in this method,
   * and record the target offset in the first call of
   * {@link #latestOffset(Offset, ReadLimit) latestOffset}.
   */
  void prepareForTriggerAvailableNow();
}
