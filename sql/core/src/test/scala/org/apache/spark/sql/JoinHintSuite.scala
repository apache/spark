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

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class JoinHintSuite extends PlanTest with SharedSQLContext {
  import testImplicits._

  lazy val df = spark.range(10)
  lazy val df1 = df.selectExpr("id as a1", "id as a2")
  lazy val df2 = df.selectExpr("id as b1", "id as b2")
  lazy val df3 = df.selectExpr("id as c1", "id as c2")

  def verifyJoinHint(df: DataFrame, expectedHints: Seq[JoinHint]): Unit = {
    val optimized = df.queryExecution.optimizedPlan
    val joinHints = optimized collect {
      case Join(_, _, _, _, hint) => hint
      case _: ResolvedHint => fail("ResolvedHint should not appear after optimize.")
    }
    assert(joinHints == expectedHints)
  }

  test("single join") {
    verifyJoinHint(
      df.hint("broadcast").join(df, "id"),
      JoinHint(
        Some(HintInfo(broadcast = true)),
        None) :: Nil
    )
    verifyJoinHint(
      df.join(df.hint("broadcast"), "id"),
      JoinHint(
        None,
        Some(HintInfo(broadcast = true))) :: Nil
    )
  }

  test("multiple joins") {
    verifyJoinHint(
      df1.join(df2.hint("broadcast").join(df3, 'b1 === 'c1).hint("broadcast"), 'a1 === 'c1),
      JoinHint(
        None,
        Some(HintInfo(broadcast = true))) ::
        JoinHint(
          Some(HintInfo(broadcast = true)),
          None) :: Nil
    )
    verifyJoinHint(
      df1.hint("broadcast").join(df2, 'a1 === 'b1).hint("broadcast").join(df3, 'a1 === 'c1),
      JoinHint(
        Some(HintInfo(broadcast = true)),
        None) ::
        JoinHint(
          Some(HintInfo(broadcast = true)),
          None) :: Nil
    )
  }

  test("hint scope") {
    withTempView("a", "b", "c") {
      df1.createOrReplaceTempView("a")
      df2.createOrReplaceTempView("b")
      verifyJoinHint(
        sql(
          """
            |select /*+ broadcast(a, b)*/ * from (
            |  select /*+ broadcast(b)*/ * from a join b on a.a1 = b.b1
            |) a join (
            |  select /*+ broadcast(a)*/ * from a join b on a.a1 = b.b1
            |) b on a.a1 = b.b1
          """.stripMargin),
        JoinHint(
          Some(HintInfo(broadcast = true)),
          Some(HintInfo(broadcast = true))) ::
          JoinHint(
            None,
            Some(HintInfo(broadcast = true))) ::
          JoinHint(
            Some(HintInfo(broadcast = true)),
            None) :: Nil
      )
    }
  }

  test("hints prevent join reorder") {
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true", SQLConf.JOIN_REORDER_ENABLED.key -> "true") {
      withTempView("a", "b", "c") {
        df1.createOrReplaceTempView("a")
        df2.createOrReplaceTempView("b")
        df3.createOrReplaceTempView("c")
        verifyJoinHint(
          sql("select /*+ broadcast(a, c)*/ * from a, b, c " +
            "where a.a1 = b.b1 and b.b1 = c.c1"),
          JoinHint(
            None,
            Some(HintInfo(broadcast = true))) ::
            JoinHint(
              Some(HintInfo(broadcast = true)),
              None) :: Nil
        )
        verifyJoinHint(
          sql("select /*+ broadcast(a, c)*/ * from a, c, b " +
            "where a.a1 = b.b1 and b.b1 = c.c1"),
          JoinHint.NONE ::
            JoinHint(
              Some(HintInfo(broadcast = true)),
              Some(HintInfo(broadcast = true))) :: Nil
        )
        verifyJoinHint(
          sql("select /*+ broadcast(b, c)*/ * from a, c, b " +
            "where a.a1 = b.b1 and b.b1 = c.c1"),
          JoinHint(
            None,
            Some(HintInfo(broadcast = true))) ::
            JoinHint(
              None,
              Some(HintInfo(broadcast = true))) :: Nil
        )

        verifyJoinHint(
          df1.join(df2, 'a1 === 'b1 && 'a1 > 5).hint("broadcast")
            .join(df3, 'b1 === 'c1 && 'a1 < 10),
          JoinHint(
            Some(HintInfo(broadcast = true)),
            None) ::
            JoinHint.NONE :: Nil
        )

        verifyJoinHint(
          df1.join(df2, 'a1 === 'b1 && 'a1 > 5).hint("broadcast")
            .join(df3, 'b1 === 'c1 && 'a1 < 10)
            .join(df, 'b1 === 'id),
          JoinHint.NONE ::
            JoinHint(
              Some(HintInfo(broadcast = true)),
              None) ::
            JoinHint.NONE :: Nil
        )
      }
    }
  }

  test("intersect/except") {
    val dfSub = spark.range(2)
    verifyJoinHint(
      df.hint("broadcast").except(dfSub).join(df, "id"),
      JoinHint(
        Some(HintInfo(broadcast = true)),
        None) ::
        JoinHint.NONE :: Nil
    )
    verifyJoinHint(
      df.join(df.hint("broadcast").intersect(dfSub), "id"),
      JoinHint(
        None,
        Some(HintInfo(broadcast = true))) ::
        JoinHint.NONE :: Nil
    )
  }

  test("hint merge") {
    verifyJoinHint(
      df.hint("broadcast").filter('id > 2).hint("broadcast").join(df, "id"),
      JoinHint(
        Some(HintInfo(broadcast = true)),
        None) :: Nil
    )
    verifyJoinHint(
      df.join(df.hint("broadcast").limit(2).hint("broadcast"), "id"),
      JoinHint(
        None,
        Some(HintInfo(broadcast = true))) :: Nil
    )
  }

  test("nested hint") {
    verifyJoinHint(
      df.hint("broadcast").hint("broadcast").filter('id > 2).join(df, "id"),
      JoinHint(
        Some(HintInfo(broadcast = true)),
        None) :: Nil
    )
  }

  test("hints prevent cost-based join reorder") {
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true", SQLConf.JOIN_REORDER_ENABLED.key -> "true") {
      val join = df.join(df, "id")
      val broadcasted = join.hint("broadcast")
      verifyJoinHint(
        join.join(broadcasted, "id").join(broadcasted, "id"),
        JoinHint(
          None,
          Some(HintInfo(broadcast = true))) ::
          JoinHint(
            None,
            Some(HintInfo(broadcast = true))) ::
          JoinHint.NONE :: JoinHint.NONE :: JoinHint.NONE :: Nil
      )
    }
  }
}
