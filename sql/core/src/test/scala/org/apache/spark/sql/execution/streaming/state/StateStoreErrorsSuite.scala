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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.SparkFunSuite

class StateStoreErrorsSuite extends SparkFunSuite {

  test("SPARK-58945: state store mismatch reports schema details") {
    checkError(
      exception = StateStoreErrors.stateStoreColumnFamilyMismatch(
        "state", "old_schema", "new_schema"),
      condition = "STATE_STORE_COLUMN_FAMILY_SCHEMA_INCOMPATIBLE",
      parameters = Map(
        "colFamilyName" -> "state",
        "oldSchema" -> "old_schema",
        "newSchema" -> "new_schema"))
  }
}
