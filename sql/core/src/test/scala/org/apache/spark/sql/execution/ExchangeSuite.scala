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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.test.SharedSQLContext

class ExchangeSuite extends SparkPlanTest with SharedSQLContext {
  import testImplicits.localSeqToDataFrameHolder

  test("shuffling UnsafeRows in exchange") {
    val input = (1 to 1000).map(Tuple1.apply)
    checkAnswer(
      input.toDF(),
      plan => ConvertToSafe(Exchange(SinglePartition, ConvertToUnsafe(plan))),
      input.map(Row.fromTuple)
    )
  }
}
