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

import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameHintSuite extends AnalysisTest with SharedSparkSession {
  import testImplicits._
  lazy val df = spark.range(10)

  private def check(df: Dataset[_], expected: LogicalPlan) = {
    comparePlans(
      df.queryExecution.logical,
      expected
    )
  }

  test("various hint parameters") {
    check(
      df.hint("hint1"),
      UnresolvedHint("hint1", Seq(),
        df.logicalPlan
      )
    )

    check(
      df.hint("hint1", 1, "a"),
      UnresolvedHint("hint1", Seq(1, "a"), df.logicalPlan)
    )

    check(
      df.hint("hint1", 1, $"a"),
      UnresolvedHint("hint1", Seq(1, $"a"),
        df.logicalPlan
      )
    )

    check(
      df.hint("hint1", Seq(1, 2, 3), Seq($"a", $"b", $"c")),
      UnresolvedHint("hint1", Seq(Seq(1, 2, 3), Seq($"a", $"b", $"c")),
        df.logicalPlan
      )
    )
  }

  test("coalesce and repartition hint") {
    check(
      df.hint("COALESCE", 10),
      UnresolvedHint("COALESCE", Seq(10), df.logicalPlan))

    check(
      df.hint("REPARTITION", 100),
      UnresolvedHint("REPARTITION", Seq(100), df.logicalPlan))

    check(
      df.hint("REPARTITION", 10, $"id".expr),
      UnresolvedHint("REPARTITION", Seq(10, $"id".expr), df.logicalPlan))

    check(
      df.hint("REPARTITION_BY_RANGE", $"id".expr),
      UnresolvedHint("REPARTITION_BY_RANGE", Seq($"id".expr), df.logicalPlan))

    check(
      df.hint("REPARTITION_BY_RANGE", 10, $"id".expr),
      UnresolvedHint("REPARTITION_BY_RANGE", Seq(10, $"id".expr), df.logicalPlan))
  }
}
