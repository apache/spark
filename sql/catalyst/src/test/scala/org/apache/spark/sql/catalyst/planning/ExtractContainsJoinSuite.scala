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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.sql.catalyst.analysis.TestRelations
import org.apache.spark.sql.catalyst.dsl.expressions.upper
import org.apache.spark.sql.catalyst.expressions.{Concat, Like, Literal}
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint}
import org.apache.spark.sql.internal.SQLConf

class ExtractContainsJoinSuite extends PlanTest {

  private val relation = TestRelations.testRelation2

  test("SPARK-38238: extract contains pattern") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.CONTAINS_JOIN_ENABLED.key -> "true") {
      // c1 LIKE concat('%' || upper(c1) || ' ' || '%')
      // upper(c1#741) LIKE concat(concat(concat(concat(% , upper(c1#785)),  ), %)
      val input1 = Concat(
        Seq(
          Concat(Seq(Concat(Seq(Literal("%"), upper(relation.output(1)))), Literal(" "))),
          Literal("%")))
      // c1 || ' '
      val output1 = Concat(Seq(upper(relation.output(1)), Literal(" ")))

      // concat(concat(concat(concat(% , upper(TITLE#831)),  ), %))
      val input2 =
        Concat(
          Seq(
            Concat(
              Seq(
                Concat(Seq(
                  Concat(Seq(Concat(Seq(Literal("%"), Literal(" "))), upper(relation.output(1)))),
                  Literal(" "))),
                Literal("%")))))
      val output2 =
        Concat(Seq(Literal(" "), upper(relation.output(1)), Literal(" ")))

      val inputAndExpects = Array((input1, output1), (input2, output2))
      val leftExpr = upper(relation.output(0))
      inputAndExpects.foreach { case (e1, e2) =>
        val join = Join(relation, relation, Inner, Some(new Like(leftExpr, e1)), JoinHint.NONE)

        val ExtractContainsJoin(_, leftKey, rightKey, _, _, buildSide, _, _, _) = join
        assert(leftKey == leftExpr)
        assert(rightKey == e2)
        assert(buildSide == BuildRight)
      }
    }
  }
}
