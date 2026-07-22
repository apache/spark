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

package org.apache.spark.sql.connector

/**
 * Runs the scoped-replace coverage against a delta-based (merge-on-read) row-level table, which
 * lowers `REPLACE USING` to a [[org.apache.spark.sql.catalyst.plans.logical.WriteDelta]] node.
 */
class DeltaBasedReplaceUsingTableSuite extends ReplaceUsingTableSuiteBase {

  override protected def isDeltaBasedReplace: Boolean = true

  override protected def extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String](super.extraTableProps)
    props.put("supports-deltas", "true")
    props
  }
}
