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

package org.apache.spark.sql.columnar

import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow

class ColumnStatsSuite extends FunSuite {
  test("Boolean") {
    val stats = new BooleanColumnStats
    val row = new GenericMutableRow(1)

    row(0) = false
    stats.gatherStats(row, 0)
    assert(stats.lowerBound === false)
    assert(stats.upperBound === false)

    row(0) = true
    stats.gatherStats(row, 0)
    assert(stats.lowerBound === false)
    assert(stats.upperBound === true)
  }
}
