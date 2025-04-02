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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.catalyst.expressions.{Cast, EqualTo}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowHiveTest

/**
 * A set of tests that validate type promotion and coercion rules.
 */
@SlowHiveTest
class HiveTypeCoercionSuite extends HiveComparisonTest {
  val baseTypes = if (SQLConf.get.ansiEnabled) {
    Seq(
    ("1", "1"),
    ("1.0", "CAST(1.0 AS DOUBLE)"),
    ("1S", "1S"),
    ("1Y", "1Y"))
  } else {
    Seq(
      ("1", "1"),
      ("1.0", "CAST(1.0 AS DOUBLE)"),
      ("1L", "1L"),
      ("1S", "1S"),
      ("1Y", "1Y"),
      ("'1'", "'1'"))
  }

  baseTypes.foreach { case (ni, si) =>
    baseTypes.foreach { case (nj, sj) =>
      createQueryTest(s"$ni + $nj", s"SELECT $si + $sj FROM src LIMIT 1")
    }
  }

  val nullVal = "null"
  baseTypes.init.foreach { case (i, s) =>
    createQueryTest(s"case when then $i else $nullVal end ",
      s"SELECT case when true then $s else $nullVal end FROM src limit 1")
    createQueryTest(s"case when then $nullVal else $i end ",
      s"SELECT case when true then $nullVal else $s end FROM src limit 1")
  }

  test("[SPARK-2210] boolean cast on boolean value should be removed") {
    val q = "select cast(cast(key=0 as boolean) as boolean) from src"
    val project = TestHive.sql(q).queryExecution.sparkPlan.collect {
      case e: ProjectExec => e
    }.head

    // No cast expression introduced
    project.transformAllExpressions { case c: Cast =>
      fail(s"unexpected cast $c")
      c
    }

    // Only one equality check
    var numEquals = 0
    project.transformAllExpressions { case e: EqualTo =>
      numEquals += 1
      e
    }
    assert(numEquals === 1)
  }
}
