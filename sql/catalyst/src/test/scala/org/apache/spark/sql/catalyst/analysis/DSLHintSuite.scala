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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

class DSLHintSuite extends AnalysisTest {
  lazy val a = 'a.int
  lazy val b = 'b.string
  lazy val c = 'c.string
  lazy val r1 = LocalRelation(a, b, c)

  test("various hint parameters") {
    comparePlans(
      r1.hint("hint1"),
      UnresolvedHint("hint1", Seq(), r1)
    )

    comparePlans(
      r1.hint("hint1", 1, "a"),
      UnresolvedHint("hint1", Seq(1, "a"), r1)
    )

    comparePlans(
      r1.hint("hint1", 1, $"a"),
      UnresolvedHint("hint1", Seq(1, $"a"), r1)
    )

    comparePlans(
      r1.hint("hint1", Seq(1, 2, 3), Seq($"a", $"b", $"c")),
      UnresolvedHint("hint1", Seq(Seq(1, 2, 3), Seq($"a", $"b", $"c")), r1)
    )
  }
}
