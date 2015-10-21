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

import org.apache.spark.sql.{SQLConf, AnalysisException}
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.hive.test.TestHive

/**
 * A test suite about the IN /NOT IN /EXISTS / NOT EXISTS subquery.
 */
abstract class HiveSemiJoinSuite extends HiveComparisonTest with BeforeAndAfter {
  import org.apache.spark.sql.hive.test.TestHive.implicits._
  import org.apache.spark.sql.hive.test.TestHive._

  private val confSortMerge = TestHive.getConf(SQLConf.SORTMERGE_JOIN)
  private val confBroadcastJoin = TestHive.getConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD)
  private val confCodegen = TestHive.getConf(SQLConf.CODEGEN_ENABLED)
  private val confTungsten = TestHive.getConf(SQLConf.TUNGSTEN_ENABLED)

  def enableSortMerge(enable: Boolean): Unit = {
    TestHive.setConf(SQLConf.SORTMERGE_JOIN, enable)
  }

  def enableBroadcastJoin(enable: Boolean): Unit = {
    if (enable) {
      TestHive.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1)
    } else {
      TestHive.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, Int.MaxValue)
    }
  }

  def enableCodeGen(enable: Boolean): Unit = {
    TestHive.setConf(SQLConf.CODEGEN_ENABLED, enable)
  }

  def enableTungsten(enable: Boolean): Unit = {
    TestHive.setConf(SQLConf.TUNGSTEN_ENABLED, enable)
  }

  override def beforeAll() {
    // override this method to update the configuration
    TestHive.cacheTables = true
  }

  override def afterAll() {
    // restore the configuration
    TestHive.setConf(SQLConf.SORTMERGE_JOIN, confSortMerge)
    TestHive.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, confBroadcastJoin)
    TestHive.setConf(SQLConf.CODEGEN_ENABLED, confCodegen)
    TestHive.setConf(SQLConf.TUNGSTEN_ENABLED, confTungsten)
    TestHive.cacheTables = false
  }

  ignore("reference the expression `min(b.value)` that required implicit change the outer query") {
    sql("""select b.key, min(b.value)
      |from src b
      |group by b.key
      |having exists ( select a.key
      |from src a
      |where a.value > 'val_9' and a.value = min(b.value))""".stripMargin)
  }

  ignore("multiple reference the outer query variables") {
    sql("""select key, value, count(*)
      |from src b
      |group by key, value
      |having count(*) in (
      |  select count(*)
      |  from src s1
      |  where s1.key > '9' and s1.value = b.value
      |  group by s1.key)""".stripMargin)
  }

  // IN Subquery Unit tests
  createQueryTest("(unrelated)WHERE clause with IN #1",
    """select *
    |from src
    |where key in (select key from src)
    |order by key, value LIMIT 5""".stripMargin)

  createQueryTest("(unrelated)WHERE clause with NOT IN #1",
    """select *
    |from src
    |where key not in (select key from src)
    |order by key, value LIMIT 5""".stripMargin)

  createQueryTest("(unrelated)WHERE clause with IN #2",
    """select *
    |from src
    |where src.key in (select t.key from src t)
    |order by key, value LIMIT 5""".stripMargin)

  createQueryTest("(unrelated)WHERE clause with NOT IN #2",
    """select *
    |from src
    |where src.key not in (select t.key % 193 from src t)
    |order by key, value LIMIT 5""".stripMargin)

  createQueryTest(
    "(unrelated)WHERE clause with IN #3",
    """select *
      |from src
      |where src.key in (select key from src s1 where s1.key > 9)
      |order by key, value LIMIT 5""".stripMargin)

  createQueryTest(
    "(unrelated)WHERE clause with NOT IN #3",
    """select *
      |from src
      |where src.key not in (select key from src s1 where s1.key > 9)
      |order by key, value LIMIT 5""".stripMargin)

  createQueryTest(
    "(unrelated)WHERE clause with IN #4",
    """select *
      |from src
      |where src.key in (select max(s1.key) from src s1 group by s1.value)
      |order by key, value LIMIT 5""".stripMargin)

  createQueryTest(
    "(unrelated)WHERE clause with NOT IN #4",
    """select *
      |from src
      |where src.key not in (select max(s1.key) % 31 from src s1 group by s1.value)
      |order by key, value LIMIT 5""".stripMargin)

  createQueryTest(
    "(unrelated)WHERE clause with IN #5",
    """select *
      |from src
      |where src.key in
      |(select max(s1.key) from src s1 group by s1.value having max(s1.key) > 3)
      |order by key, value LIMIT 5""".stripMargin)

  createQueryTest(
    "(unrelated)WHERE clause with NOT IN #5",
    """select *
      |from src
      |where src.key not in
      |(select max(s1.key) from src s1 group by s1.value having max(s1.key) > 3)
      |order by key, value LIMIT 5""".stripMargin)

  createQueryTest(
    "(unrelated)WHERE clause with IN #6",
    """select *
      |from src
      |where src.key in
      |(select max(s1.key) from src s1 group by s1.value having max(s1.key) > 3)
      |      and src.key > 10
      |order by key, value LIMIT 5""".stripMargin)

  createQueryTest(
    "(unrelated)WHERE clause with NOT IN #6",
    """select *
      |from src
      |where src.key not in
      |(select max(s1.key) % 31 from src s1 group by s1.value having max(s1.key) > 3)
      |      and src.key > 10
      |order by key, value LIMIT 5""".stripMargin)

  createQueryTest(
    "(unrelated)WHERE clause with IN #7",
    """select *
      |  from src b
      |where b.key in
      |(select count(*)
      |  from src a
      |  where a.key > 100
      |) and b.key < 200
      |order by key, value
      |LIMIT 5""".stripMargin)

  createQueryTest(
    "(unrelated)WHERE clause with NOT IN #7",
    """select *
      |  from src b
      |where b.key not in
      |(select count(*)
      |  from src a
      |  where a.key > 100
      |) and b.key < 200
      |order by key, value
      |LIMIT 5""".stripMargin)

  createQueryTest(
    "(correlated)WHERE clause with IN #1",
    """select *
      |from src b
      |where b.key in
      |        (select a.key
      |         from src a
      |         where b.value = a.value and a.key > 9
      |        )
      |order by key, value
      |LIMIT 5""".stripMargin)

  createQueryTest(
    "(correlated)WHERE clause with NOT IN #1",
    """select *
    |from src b
    |where b.key not in
    |        (select a.key
    |         from src a
    |         where b.value = a.value and a.key > 9
    |        )
    |order by key, value
    |LIMIT 5"""
      .stripMargin)

  createQueryTest(
    "(correlated)WHERE clause with IN #2",
    """select *
    |from src b
    |where b.key in
    |        (select a.key
    |         from src a
    |         where b.value = a.value and a.key > 9
    |        )
    |and b.key > 15
    |order by key, value
    |LIMIT 5""".stripMargin)

  createQueryTest(
    "(correlated)WHERE clause with NOT IN #2",
    """select *
    |from src b
    |where b.key not in
    |        (select a.key
    |         from src a
    |         where b.value = a.value and a.key > 9
    |        )
    |and b.key > 15
    |order by key, value
    |LIMIT 5""".
      stripMargin)

  createQueryTest(
    "(correlated)WHERE clause with EXISTS #1",
    """select *
    |from src b
    |where EXISTS
    |        (select a.key
    |         from src a
    |         where b.value = a.value and a.key > 9
    |        )
    |order by key, value
    |LIMIT 5""".
      stripMargin)

  createQueryTest(
    "(correlated)WHERE clause with NOT EXISTS #1",
    """select *
    |from src b
    |where NOT EXISTS
    |        (select a.key
    |         from src a
    |         where b.value = a.value and a.key > 9
    |        )
    |order by key, value
    |LIMIT 5""".stripMargin)

  createQueryTest(
    "(correlated)WHERE clause with EXISTS #2",
    """select *
    |from src b
    |where EXISTS
    |        (select a.key
    |         from src a
    |         where b.value = a.value and a.key > 9
    |        )
    |and b.key > 15
    |order by key, value
    |LIMIT 5""".
      stripMargin)

  createQueryTest(
    "(correlated)WHERE clause with NOT EXISTS #2",
    """select *
    |from src b
    |where NOT EXISTS
    |        (select a.key % 291
    |         from src a
    |         where b.value = a.value and a.key > 9
    |        )
    |and b.key > 15
    |order by key, value
    |LIMIT 5""".
      stripMargin)
}

class SemiJoinHashJoin extends HiveSemiJoinSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableSortMerge(false)
    enableTungsten(false)
    enableCodeGen(false)
  }
}

class SemiJoinHashJoinTungsten extends HiveSemiJoinSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableSortMerge(false)
    enableTungsten(true)
    enableCodeGen(true)
  }
}

class SemiJoinSortMerge extends HiveSemiJoinSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableSortMerge(true)
    enableTungsten(false)
    enableCodeGen(false)
  }
}

class SemiJoinSortMergeTungsten extends HiveSemiJoinSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableSortMerge(true)
    enableTungsten(true)
    enableCodeGen(true)
  }
}

class SemiJoinBroadcast extends HiveSemiJoinSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableBroadcastJoin(true)
    enableTungsten(false)
    enableCodeGen(false)
  }
}

class SemiJoinBroadcastCodeGenTungsten extends HiveSemiJoinSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableBroadcastJoin(true)
    enableTungsten(true)
    enableCodeGen(true)
  }
}

// TODO we should move this to module sql/core, however, the SQLParser doesn't support
// in/exists clause right now.
class SubqueryPredicateSemanticCheckSuite extends HiveComparisonTest with BeforeAndAfter {
  import org.apache.spark.sql.hive.test.TestHive._

  override def beforeAll() {
    TestHive.cacheTables = true
  }

  override def afterAll() {
    TestHive.cacheTables = false
  }

  test("semi join key is not in the filter expressions") {
    assert(intercept[AnalysisException] {
      sql(
        """select *
          |from src b
          |where b.value in
          |        (select concat("val_", a.key % b.key)
          |         from src a
          |        )
          |order by key, value
          |LIMIT 5""".stripMargin)
    }.getMessage.contains("Cannot resolve the projection"))

    assert(intercept[AnalysisException] {
      sql(
        """select *
          |from src b
          |where b.key in
          |        (select max(a.key) % b.key
          |         from src a
          |         group by a.value
          |        )
          |order by key, value
          |LIMIT 5""".stripMargin)
    }.getMessage.contains("Cannot resolve the aggregation"))

    assert(intercept[AnalysisException] {
      sql(
        """select *
          |from src b
          |where b.key in
          |        (select max(a.key) % b.key
          |         from src a
          |         group by a.value
          |         having count(a.value) > 2
          |        )
          |order by key, value
          |LIMIT 5""".stripMargin)
    }.getMessage.contains("Cannot resolve the aggregation"))
  }

  test("Expect only 1 projection in In Subquery Expression") {
    assert(intercept[AnalysisException] {
      sql(
        """select *
          |from src b
          |where b.key not in
          |        (select a.key, a.value
          |         from src a
          |         where a.key > 9
          |        )
          |order by key, value
          |LIMIT 5""".stripMargin)
    }.getMessage.contains("Expect only 1 projection in In SubQuery Expression"))
  }

  test("Exist clause should be correlated") {
    assert(intercept[AnalysisException] {
      sql(
        """select *
          |from src
          |where EXISTS
          |(select max(s1.key) from src s1 group by s1.value having max(s1.key) > 3)
          |order by key, value LIMIT 5""".stripMargin)
    }.getMessage.contains("Exists/Not Exists operator SubQuery must be correlated"))

    assert(intercept[AnalysisException] {
      sql(
        """select *
          |from src
          |where NOT EXISTS
          |(select value from src where key > 3)
          |order by key, value LIMIT 5""".stripMargin)
    }.getMessage.contains("Exists/Not Exists operator SubQuery must be correlated"))
  }
}
