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

package org.apache.spark.sql.pipelines.autocdc

/** Shared helpers for the out-of-order CDC merge implementations (SCD Type 1 and Type 2). */
private[autocdc] object OutOfOrderCdcMergeUtils {

  /**
   * Build a synthetic column name with a UUID suffix so it cannot collide with any user
   * column. Intended for transient columns attached during merge processing (e.g. holding
   * intermediate aggregation outputs, carrying per-key state through a join, etc.).
   *
   * Each invocation produces a fresh name, so callers should remember the returned string if
   * they need to reference the same column from multiple sites within a single merge plan.
   */
  def tempColName(prefix: String): String =
    s"${prefix}_${java.util.UUID.randomUUID().toString.replace("-", "_")}"
}
