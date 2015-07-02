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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.dsl.expressions._

class SortSuite extends SparkPlanTest {

  // This test was originally added as an example of how to use [[SparkPlanTest]];
  // it's not designed to be a comprehensive test of ExternalSort.
  test("basic sorting using ExternalSort") {

    val input = Seq(
      ("Hello", 4, 2.0),
      ("Hello", 1, 1.0),
      ("World", 8, 3.0)
    )

    checkAnswer(
      input.toDF("a", "b", "c"),
      ExternalSort('a.asc :: 'b.asc :: Nil, global = false, _: SparkPlan),
      input.sorted)

    checkAnswer(
      input.toDF("a", "b", "c"),
      ExternalSort('b.asc :: 'a.asc :: Nil, global = false, _: SparkPlan),
      input.sortBy(t => (t._2, t._1)))
  }
}
