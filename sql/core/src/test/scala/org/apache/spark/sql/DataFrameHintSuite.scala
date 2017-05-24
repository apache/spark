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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedHint
import org.apache.spark.sql.test.SharedSQLContext

class DataFrameHintSuite extends PlanTest with SharedSQLContext {
  import testImplicits._
  lazy val df = spark.range(10)

  test("various hint parameters") {
    comparePlans(
      df.hint("hint1").queryExecution.logical,
      UnresolvedHint("hint1", Seq(),
        df.logicalPlan
      )
    )

    comparePlans(
      df.hint("hint1", 1, "a").queryExecution.logical,
      UnresolvedHint("hint1", Seq(1, "a"), df.logicalPlan)
    )

    comparePlans(
      df.hint("hint1", 1, $"a").queryExecution.logical,
      UnresolvedHint("hint1", Seq(1, $"a"),
        df.logicalPlan
      )
    )

    comparePlans(
      df.hint("hint1", Seq(1, 2, 3), Seq($"a", $"b", $"c")).queryExecution.logical,
      UnresolvedHint("hint1", Seq(Seq(1, 2, 3), Seq($"a", $"b", $"c")),
        df.logicalPlan
      )
    )
  }
}
