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

package org.apache.spark.sql.catalyst.plans

/**
 * Acceptance lists for the `NEAREST BY` join API.
 */
private[sql] object NearestByJoinValidation {

  /** Upper bound on `numResults`. Mirrors the K-overload limit of `MaxMinByK`. */
  val MaxNumResults: Int = 100000

  /**
   * Strings accepted by `joinType` after lower-casing and stripping `_` (so e.g. `LEFT_OUTER`
   * canonicalizes to `leftouter`). Every consumer must apply the same canonicalization before
   * checking membership.
   */
  val SupportedJoinTypes: Seq[String] = Seq("inner", "leftouter", "left")

  /** Display form for `supported` in `NEAREST_BY_JOIN.UNSUPPORTED_JOIN_TYPE` error messages. */
  val SupportedJoinTypeDisplay: String = "'INNER', 'LEFT OUTER'"

  /** Strings accepted by `mode`. Lower-cased before membership check. */
  val SupportedModes: Seq[String] = Seq("approx", "exact")

  /** Strings accepted by `direction`. Lower-cased before membership check. */
  val SupportedDirections: Seq[String] = Seq("distance", "similarity")
}
