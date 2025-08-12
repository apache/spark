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

package org.apache.spark.internal;

/**
 * All structured logging `keys` used in `MDC` must be extends `LogKey`
 * <p>
 *
 * `LogKey`s serve as identifiers for mapped diagnostic contexts (MDC) within logs.
 * Follow these guidelines when adding a new LogKey:
 * <ul>
 *   <li>
 *     Define all structured logging keys in `LogKeys.java`, and sort them alphabetically for
 *     ease of search.
 *   </li>
 *   <li>
 *     Use `UPPER_SNAKE_CASE` for key names.
 *   </li>
 *   <li>
 *     Key names should be both simple and broad, yet include specific identifiers like `STAGE_ID`,
 *     `TASK_ID`, and `JOB_ID` when needed for clarity. For instance, use `MAX_ATTEMPTS` as a
 *     general key instead of creating separate keys for each scenario such as
 *     `EXECUTOR_STATE_SYNC_MAX_ATTEMPTS` and `MAX_TASK_FAILURES`.
 *     This balances simplicity with the detail needed for effective logging.
 *   </li>
 *   <li>
 *     Use abbreviations in names if they are widely understood,
 *     such as `APP_ID` for APPLICATION_ID, and `K8S` for KUBERNETES.
 *   </li>
 *   <li>
 *     For time-related keys, use milliseconds as the unit of time.
 *   </li>
 * </ul>
 */
public interface LogKey {
  String name();
}
