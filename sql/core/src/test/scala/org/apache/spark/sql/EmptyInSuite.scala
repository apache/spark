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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class EmptyInSuite extends QueryTest
with SharedSparkSession {
  import testImplicits._

  val row = identity[(java.lang.Integer, java.lang.Double)](_)

  lazy val t = Seq(
    row((1, 1.0)),
    row((null, 2.0))).toDF("a", "b")

  test("IN with empty list") {
    // This test has to be written in scala to construct a literal empty IN list, since that
    // isn't valid syntax in SQL.
    val emptylist = Seq.empty[Literal]

    Seq(true, false).foreach { legacyNullInBehavior =>
      // To observe execution behavior, disable the OptimizeIn rule which optimizes away empty lists
      Seq(true, false).foreach { disableOptimizeIn =>
        // Disable ConvertToLocalRelation since it would collapse the evaluation of the IN
        // expression over the LocalRelation
        var excludedRules = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
        if (disableOptimizeIn) {
          excludedRules += ",org.apache.spark.sql.catalyst.optimizer.OptimizeIn"
        }
        withSQLConf(
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excludedRules,
          SQLConf.LEGACY_NULL_IN_EMPTY_LIST_BEHAVIOR.key -> legacyNullInBehavior.toString) {
          val expectedResultForNullInEmpty =
            if (legacyNullInBehavior) null else false
          val df = t.select(col("a"), col("a").isin(emptylist: _*))
          checkAnswer(
            df,
            Row(1, false) :: Row(null, expectedResultForNullInEmpty) :: Nil)
        }
      }
    }
  }

  test("IN empty list behavior conf defaults") {
    // Currently the fixed behavior is enabled when ANSI is on, and the legacy behavior when
    // ANSI is off.
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        val legacyNullInBehavior = !ansiEnabled
        assert(SQLConf.get.legacyNullInEmptyBehavior == legacyNullInBehavior)

        val emptylist = Seq.empty[Literal]
        val df = t.select(col("a"), col("a").isin(emptylist: _*))
        val expectedResultForNullInEmpty =
          if (legacyNullInBehavior) null else false
        checkAnswer(
          df,
          Row(1, false) :: Row(null, expectedResultForNullInEmpty) :: Nil)
      }
    }
  }
}
