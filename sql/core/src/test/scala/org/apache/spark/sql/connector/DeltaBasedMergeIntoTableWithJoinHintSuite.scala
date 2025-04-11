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

package org.apache.spark.sql.connector

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, CommandResult, HintInfo, Join, JoinHint, NO_BROADCAST_AND_REPLICATION, ResolvedHint, SHUFFLE_HASH, SHUFFLE_MERGE, SHUFFLE_REPLICATE_NL}
import org.apache.spark.sql.execution.{CommandResultExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, ReplaceDataExec, WriteDeltaExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, CartesianProductExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

class DeltaBasedMergeIntoTableWithJoinHintSuite extends RowLevelOperationSuiteBase {

  import testImplicits._

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props
  }

  def verifyJoinHint(df: DataFrame, expectedHints: Seq[JoinHint]): Unit = {
    val optimizedPlan = df.queryExecution.optimizedPlan
    assert(optimizedPlan.isInstanceOf[CommandResult])
    val joinHints = optimizedPlan.asInstanceOf[CommandResult].commandOptimizedLogicalPlan collect {
      case Join(_, _, _, _, hint) => hint
      case _: ResolvedHint => fail("ResolvedHint should not appear after optimize.")
    }
    assert(joinHints == expectedHints)
  }

  private def getCommandResultExec(df: DataFrame): CommandResultExec = {
    assert(df.queryExecution.executedPlan.isInstanceOf[CommandResultExec])
    df.queryExecution.executedPlan.asInstanceOf[CommandResultExec]
  }

  def verifyBroadcastHashJoinExec(query: SparkPlan, num: Int): Unit = {
    assert(query.isInstanceOf[AdaptiveSparkPlanExec])
    val adaptiveSparkPlanExec = query.asInstanceOf[AdaptiveSparkPlanExec]
    val actualNum = collect(adaptiveSparkPlanExec.inputPlan) {
      case p: BroadcastHashJoinExec => p
    }.size
    assert(actualNum == num)
  }

  def verifySortMergeJoinExec(query: SparkPlan, num: Int): Unit = {
    assert(query.isInstanceOf[AdaptiveSparkPlanExec])
    val adaptiveSparkPlanExec = query.asInstanceOf[AdaptiveSparkPlanExec]
    val actualNum = collect(adaptiveSparkPlanExec.inputPlan) {
      case p: SortMergeJoinExec => p
    }.size
    assert(actualNum == num)
  }

  def verifyShuffleHashJoinExec(query: SparkPlan, num: Int): Unit = {
    assert(query.isInstanceOf[AdaptiveSparkPlanExec])
    val adaptiveSparkPlanExec = query.asInstanceOf[AdaptiveSparkPlanExec]
    val actualNum = collect(adaptiveSparkPlanExec.inputPlan) {
      case p: ShuffledHashJoinExec => p
    }.size
    assert(actualNum == num)
  }

  def verifyCartesianProductExec(query: SparkPlan, num: Int): Unit = {
    assert(query.isInstanceOf[AdaptiveSparkPlanExec])
    val adaptiveSparkPlanExec = query.asInstanceOf[AdaptiveSparkPlanExec]
    val actualNum = collect(adaptiveSparkPlanExec.inputPlan) {
      case p: CartesianProductExec => p
    }.size
    assert(actualNum == num)
  }

  // calling buildAppendDataPlan begin

  test("merge into empty table with NOT MATCHED clause (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join doesn't support broadcast left
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ broadcast(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyBroadcastHashJoinExec(appendDataExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with NOT MATCHED clause (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join supports broadcast right
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ broadcast(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyBroadcastHashJoinExec(appendDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with NOT MATCHED clause (sort merge left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifySortMergeJoinExec(appendDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with NOT MATCHED clause (sort merge right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifySortMergeJoinExec(appendDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with NOT MATCHED clause (shuffle hash left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join doesn't support shuffle hash left
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_HASH(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_HASH))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyShuffleHashJoinExec(appendDataExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with NOT MATCHED clause (shuffle hash right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join supports shuffle hash right
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_HASH(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_HASH)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyShuffleHashJoinExec(appendDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with NOT MATCHED clause (cartesian product left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join doesn't support cartesian product
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyCartesianProductExec(appendDataExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with NOT MATCHED clause (cartesian product right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join doesn't support cartesian product
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyCartesianProductExec(appendDataExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with conditional NOT MATCHED clause (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join doesn't support broadcast left
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED AND s.pk >= 2 THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyBroadcastHashJoinExec(appendDataExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with conditional NOT MATCHED clause (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join supports broadcast right
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED AND s.pk >= 2 THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyBroadcastHashJoinExec(appendDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  // calling buildAppendDataPlanForMultipleNotMatchedActions begin

  test("merge into empty table with multiple NOT MATCHED clause (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join doesn't support broadcast left
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED AND s.pk >= 2 THEN
             | INSERT *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyBroadcastHashJoinExec(appendDataExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with multiple NOT MATCHED clause (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join supports broadcast right
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED AND s.pk >= 2 THEN
             | INSERT *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyBroadcastHashJoinExec(appendDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with multiple NOT MATCHED clause (sort merge left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED AND s.pk >= 2 THEN
             | INSERT *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifySortMergeJoinExec(appendDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with multiple NOT MATCHED clause (sort merge right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED AND s.pk >= 2 THEN
             | INSERT *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifySortMergeJoinExec(appendDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with multiple NOT MATCHED clause (shuffle hash left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join doesn't support shuffle hash left
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_HASH(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED AND s.pk >= 2 THEN
             | INSERT *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_HASH))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyShuffleHashJoinExec(appendDataExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with multiple NOT MATCHED clause (shuffle hash right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join supports shuffle hash right
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_HASH(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED AND s.pk >= 2 THEN
             | INSERT *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_HASH)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyShuffleHashJoinExec(appendDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with multiple NOT MATCHED clause (cartesian product left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join doesn't support cartesian product
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED AND s.pk >= 2 THEN
             | INSERT *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyCartesianProductExec(appendDataExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with multiple NOT MATCHED clause (cartesian product right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left anti join doesn't support cartesian product
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED AND s.pk >= 2 THEN
             | INSERT *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[AppendDataExec])
        val appendDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[AppendDataExec]
        verifyCartesianProductExec(appendDataExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  // calling buildWriteDeltaPlan begin

  test("merge into with conditional WHEN MATCHED clause (update) (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "corrupted" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 100, "software"),
          (2, 200, "finance"),
          (3, 300, "software"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED AND s.pk = 2 THEN
             | UPDATE SET *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "finance"))) // update
      }
    }
  }

  test("merge into with conditional WHEN MATCHED clause (update) (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "corrupted" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 100, "software"),
          (2, 200, "finance"),
          (3, 300, "software"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED AND s.pk = 2 THEN
             | UPDATE SET *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "finance"))) // update
      }
    }
  }

  test("merge into with conditional WHEN MATCHED clause (update) (sort merge right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "corrupted" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 100, "software"),
          (2, 200, "finance"),
          (3, 300, "software"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED AND s.pk = 2 THEN
             | UPDATE SET *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifySortMergeJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "finance"))) // update
      }
    }
  }

  test("merge into with conditional WHEN MATCHED clause (update) (sort merge left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "corrupted" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 100, "software"),
          (2, 200, "finance"),
          (3, 300, "software"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED AND s.pk = 2 THEN
             | UPDATE SET *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifySortMergeJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "finance"))) // update
      }
    }
  }

  test("merge into with conditional WHEN MATCHED clause (update) (cartesian product right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "corrupted" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 100, "software"),
          (2, 200, "finance"),
          (3, 300, "software"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED AND s.pk = 2 THEN
             | UPDATE SET *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyCartesianProductExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "finance"))) // update
      }
    }
  }

  test("merge into with conditional WHEN MATCHED clause (update) (cartesian product left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "corrupted" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 100, "software"),
          (2, 200, "finance"),
          (3, 300, "software"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED AND s.pk = 2 THEN
             | UPDATE SET *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyCartesianProductExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "finance"))) // update
      }
    }
  }

  test("merge into with conditional WHEN MATCHED clause (delete) (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "corrupted" }
            |""".stripMargin)

        Seq(1, 2, 3).toDF("pk").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED AND t.salary = 200 THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(Row(1, 100, "hr"))) // unchanged
      }
    }
  }

  test("merge into with conditional WHEN MATCHED clause (delete) (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "corrupted" }
            |""".stripMargin)

        Seq(1, 2, 3).toDF("pk").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED AND t.salary = 200 THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(Row(1, 100, "hr"))) // unchanged
      }
    }
  }

  test("merge with one NOT MATCHED BY SOURCE clause causes (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join supports broadcast right
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(1, 2).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "software"))) // unchanged
      }
    }
  }

  test("merge with one NOT MATCHED BY SOURCE clause causes (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join doesn't support broadcast left
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(1, 2).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "software"))) // unchanged
      }
    }
  }

  test("merge with one NOT MATCHED BY SOURCE clause causes (sort merge right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(1, 2).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifySortMergeJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "software"))) // unchanged
      }
    }
  }

  test("merge with one NOT MATCHED BY SOURCE clause causes (sort merge left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(1, 2).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifySortMergeJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "software"))) // unchanged
      }
    }
  }

  test("merge with one NOT MATCHED BY SOURCE clause causes (shuffle hash right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join supports shuffle hash right
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(1, 2).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_HASH(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_HASH)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyShuffleHashJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "software"))) // unchanged
      }
    }
  }

  test("merge with one NOT MATCHED BY SOURCE clause causes (shuffle hash left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join supports shuffle hash left
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(1, 2).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_HASH(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_HASH))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyShuffleHashJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "software"))) // unchanged
      }
    }
  }

  test("merge with one NOT MATCHED BY SOURCE clause causes (cartesian product right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join doesn't support cartesian product
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(1, 2).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyCartesianProductExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "software"))) // unchanged
      }
    }
  }

  test("merge with one NOT MATCHED BY SOURCE clause causes (cartesian product left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join doesn't support cartesian product
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(1, 2).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyCartesianProductExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 100, "hr"), // unchanged
            Row(2, 200, "software"))) // unchanged
      }
    }
  }

  test("merge with one conditional NOT MATCHED BY SOURCE clause (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join supports broadcast right
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(2).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
             | UPDATE SET salary = -1
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, -1, "hr"), // updated
            Row(2, 200, "software"), // unchanged
            Row(3, 300, "hr"))) // unchanged
      }
    }
  }

  test("merge with one conditional NOT MATCHED BY SOURCE clause (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join doesn't support broadcast left
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(2).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
             | UPDATE SET salary = -1
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, -1, "hr"), // updated
            Row(2, 200, "software"), // unchanged
            Row(3, 300, "hr"))) // unchanged
      }
    }
  }

  test("merge with MATCHED and NOT MATCHED BY SOURCE clauses (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join supports broadcast right
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(2).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
             | UPDATE SET salary = -1
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, -1, "hr"), // updated
            Row(3, 300, "hr"))) // unchanged
      }
    }
  }

  test("merge with MATCHED and NOT MATCHED BY SOURCE clauses (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join doesn't support broadcast left
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(2).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
             | UPDATE SET salary = -1
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, -1, "hr"), // updated
            Row(3, 300, "hr"))) // unchanged
      }
    }
  }

  test("merge with all types of clauses (broadcast right hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "salary": 400, "dep": "hr" }
          |{ "pk": 5, "salary": 500, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(3, 4, 5, 6).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      val df = sql(
        s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET t.salary = t.salary + 1
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'new')
           |WHEN NOT MATCHED BY SOURCE AND pk = 1 THEN
           | DELETE
           |""".stripMargin)

      verifyJoinHint(df, JoinHint(
        Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
        None) :: Nil)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
      val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
      verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "software"), // unchanged
          Row(3, 301, "hr"), // update
          Row(4, 401, "hr"), // update
          Row(5, 501, "hr"), // update
          Row(6, 0, "new"))) // insert
    }
  }

  test("merge with all types of clauses (broadcast left hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "salary": 400, "dep": "hr" }
          |{ "pk": 5, "salary": 500, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(3, 4, 5, 6).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      val df = sql(
        s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET t.salary = t.salary + 1
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'new')
           |WHEN NOT MATCHED BY SOURCE AND pk = 1 THEN
           | DELETE
           |""".stripMargin)

      verifyJoinHint(df, JoinHint(
        Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
        None) :: Nil)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
      val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
      verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "software"), // unchanged
          Row(3, 301, "hr"), // update
          Row(4, 401, "hr"), // update
          Row(5, 501, "hr"), // update
          Row(6, 0, "new"))) // insert
    }
  }

  test("merge with NOT MATCHED and NOT MATCHED BY SOURCE clauses (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The full outer join doesn't support broadcast right
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(2, 3, 4).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (pk, -1, 'new')
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, 200, "software"), // unchanged
            Row(3, 300, "hr"), // unchanged
            Row(4, -1, "new"))) // insert
      }
    }
  }

  test("merge with NOT MATCHED and NOT MATCHED BY SOURCE clauses (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The full outer join doesn't support broadcast left
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(2, 3, 4).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (pk, -1, 'new')
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, 200, "software"), // unchanged
            Row(3, 300, "hr"), // unchanged
            Row(4, -1, "new"))) // insert
      }
    }
  }

  test("merge with NOT MATCHED and NOT MATCHED BY SOURCE clauses (sort merge right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(2, 3, 4).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (pk, -1, 'new')
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifySortMergeJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, 200, "software"), // unchanged
            Row(3, 300, "hr"), // unchanged
            Row(4, -1, "new"))) // insert
      }
    }
  }

  test("merge with NOT MATCHED and NOT MATCHED BY SOURCE clauses (sort merge left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(2, 3, 4).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (pk, -1, 'new')
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifySortMergeJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, 200, "software"), // unchanged
            Row(3, 300, "hr"), // unchanged
            Row(4, -1, "new"))) // insert
      }
    }
  }

  test("merge with NOT MATCHED and NOT MATCHED BY SOURCE clauses (shuffle hash right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The full outer join supports shuffle hash right
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(2, 3, 4).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_HASH(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (pk, -1, 'new')
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_HASH)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyShuffleHashJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, 200, "software"), // unchanged
            Row(3, 300, "hr"), // unchanged
            Row(4, -1, "new"))) // insert
      }
    }
  }

  test("merge with NOT MATCHED and NOT MATCHED BY SOURCE clauses (shuffle hash left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The full outer join supports shuffle hash left
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(2, 3, 4).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_HASH(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (pk, -1, 'new')
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_HASH))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyShuffleHashJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, 200, "software"), // unchanged
            Row(3, 300, "hr"), // unchanged
            Row(4, -1, "new"))) // insert
      }
    }
  }

  test("merge with NOT MATCHED and NOT MATCHED BY SOURCE clauses (cartesian product right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The full outer join doesn't support cartesian product
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(2, 3, 4).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (pk, -1, 'new')
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyCartesianProductExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, 200, "software"), // unchanged
            Row(3, 300, "hr"), // unchanged
            Row(4, -1, "new"))) // insert
      }
    }
  }

  test("merge with NOT MATCHED and NOT MATCHED BY SOURCE clauses (cartesian product left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The full outer join doesn't support cartesian product
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(2, 3, 4).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (pk, -1, 'new')
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyCartesianProductExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, 200, "software"), // unchanged
            Row(3, 300, "hr"), // unchanged
            Row(4, -1, "new"))) // insert
      }
    }
  }

  test("merge with multiple NOT MATCHED BY SOURCE clauses (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join supports broadcast right
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(5, 6, 7).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
             | UPDATE SET salary = salary + 1
             |WHEN NOT MATCHED BY SOURCE AND salary = 300 THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 101, "hr"), // update
            Row(2, 200, "software"))) // unchanged
      }
    }
  }

  test("merge with multiple NOT MATCHED BY SOURCE clauses (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join doesn't support broadcast left
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        val sourceDF = Seq(5, 6, 7).toDF("pk")
        sourceDF.createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
             | UPDATE SET salary = salary + 1
             |WHEN NOT MATCHED BY SOURCE AND salary = 300 THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 101, "hr"), // update
            Row(2, 200, "software"))) // unchanged
      }
    }
  }

  test("merge with CTE (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |""".stripMargin)

        val sourceRows = Seq(
          (0, 101, "support"),
          (2, 301, "support"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""WITH cte1 AS (SELECT pk + 1 as pk, salary, dep FROM source)
             |MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString AS t
             |USING cte1 AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 101, "support"), // unchanged
            Row(2, 200, "software"))) // unchanged
      }
    }
  }

  test("merge with CTE (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |""".stripMargin)

        val sourceRows = Seq(
          (0, 101, "support"),
          (2, 301, "support"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""WITH cte1 AS (SELECT pk + 1 as pk, salary, dep FROM source)
             |MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString AS t
             |USING cte1 AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 101, "support"), // unchanged
            Row(2, 200, "software"))) // unchanged
      }
    }
  }

  test("merge with subquery as source (broadcast right hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 6, "salary": 600, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (2, 201, "support"),
        (1, 101, "support"),
        (3, 301, "support"),
        (6, 601, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      val subquery =
        s"""
           |SELECT * FROM source WHERE pk = 2
           |UNION ALL
           |SELECT * FROM source WHERE pk = 1 OR pk = 6
           |""".stripMargin

      val df = sql(
        s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString AS t
           |USING ($subquery) AS s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET *
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      verifyJoinHint(df, JoinHint(
        Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
        None) :: Nil)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
      val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
      verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "support"), // update
          Row(2, 201, "support"), // insert
          Row(6, 601, "support"))) // update
    }
  }

  test("merge with subquery as source (broadcast left hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 6, "salary": 600, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (2, 201, "support"),
        (1, 101, "support"),
        (3, 301, "support"),
        (6, 601, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      val subquery =
        s"""
           |SELECT * FROM source WHERE pk = 2
           |UNION ALL
           |SELECT * FROM source WHERE pk = 1 OR pk = 6
           |""".stripMargin

      val df = sql(
        s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString AS t
           |USING ($subquery) AS s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET *
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      verifyJoinHint(df, JoinHint(
        Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
        None) :: Nil)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
      val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
      verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "support"), // update
          Row(2, 201, "support"), // insert
          Row(6, 601, "support"))) // update
    }
  }

  test("merge cardinality check with unconditional MATCHED clause (delete)" +
    " (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The inner join supports broadcast right
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 6, "salary": 600, "dep": "software" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 101, "support"),
          (1, 102, "support"),
          (2, 201, "support"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString AS t
             |USING source AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(Row(6, 600, "software"))) // unchanged
      }
    }
  }

  test("merge cardinality check with unconditional MATCHED clause (delete)" +
    " (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The inner join supports broadcast left
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 6, "salary": 600, "dep": "software" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 101, "support"),
          (1, 102, "support"),
          (2, 201, "support"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString AS t
             |USING source AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(Row(6, 600, "software"))) // unchanged
      }
    }
  }

  test("merge cardinality check with unconditional MATCHED clause (delete)" +
    " (sort merge right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 6, "salary": 600, "dep": "software" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 101, "support"),
          (1, 102, "support"),
          (2, 201, "support"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(s) */ INTO $tableNameAsString AS t
             |USING source AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifySortMergeJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(Row(6, 600, "software"))) // unchanged
      }
    }
  }

  test("merge cardinality check with unconditional MATCHED clause (delete)" +
    " (sort merge left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 6, "salary": 600, "dep": "software" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 101, "support"),
          (1, 102, "support"),
          (2, 201, "support"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(t) */ INTO $tableNameAsString AS t
             |USING source AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifySortMergeJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(Row(6, 600, "software"))) // unchanged
      }
    }
  }

  test("merge cardinality check with unconditional MATCHED clause (delete)" +
    " (shuffle hash right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The inner join supports shuffle hash right
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 6, "salary": 600, "dep": "software" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 101, "support"),
          (1, 102, "support"),
          (2, 201, "support"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_HASH(s) */ INTO $tableNameAsString AS t
             |USING source AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_HASH)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyShuffleHashJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(Row(6, 600, "software"))) // unchanged
      }
    }
  }

  test("merge cardinality check with unconditional MATCHED clause (delete)" +
    " (shuffle hash left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The inner join supports shuffle hash left
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 6, "salary": 600, "dep": "software" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 101, "support"),
          (1, 102, "support"),
          (2, 201, "support"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_HASH(t) */ INTO $tableNameAsString AS t
             |USING source AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_HASH))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyShuffleHashJoinExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(Row(6, 600, "software"))) // unchanged
      }
    }
  }

  test("merge cardinality check with unconditional MATCHED clause (delete)" +
    " (cartesian product right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The inner join supports cartesian product
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 6, "salary": 600, "dep": "software" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 101, "support"),
          (1, 102, "support"),
          (2, 201, "support"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(s) */ INTO $tableNameAsString AS t
             |USING source AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyCartesianProductExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(Row(6, 600, "software"))) // unchanged
      }
    }
  }

  test("merge cardinality check with unconditional MATCHED clause (delete)" +
    " (cartesian product left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The inner join supports cartesian product
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 6, "salary": 600, "dep": "software" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 101, "support"),
          (1, 102, "support"),
          (2, 201, "support"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(t) */ INTO $tableNameAsString AS t
             |USING source AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyCartesianProductExec(writeDeltaExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(Row(6, 600, "software"))) // unchanged
      }
    }
  }

  test("self merge (broadcast right hint)") {
    // we can't apply hint due to cardinality check
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    val df = sql(
      s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
         |USING $tableNameAsString s
         |ON t.pk = s.pk
         |WHEN MATCHED AND t.salary = 100 THEN
         | UPDATE SET salary = t.salary + 1
         |WHEN NOT MATCHED THEN
         | INSERT *
         |""".stripMargin)

    verifyJoinHint(df, JoinHint(
      Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
      None) :: Nil)

    val commandResultExec = getCommandResultExec(df)
    assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
    val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
    verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 101, "hr"), // update
        Row(2, 200, "software"), // unchanged
        Row(3, 300, "hr"))) // unchanged
  }

  test("self merge (broadcast left hint)") {
    // we can't apply hint due to cardinality check
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    val df = sql(
      s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
         |USING $tableNameAsString s
         |ON t.pk = s.pk
         |WHEN MATCHED AND t.salary = 100 THEN
         | UPDATE SET salary = t.salary + 1
         |WHEN NOT MATCHED THEN
         | INSERT *
         |""".stripMargin)

    verifyJoinHint(df, JoinHint(
      Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
      None) :: Nil)

    val commandResultExec = getCommandResultExec(df)
    assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
    val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
    verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 101, "hr"), // update
        Row(2, 200, "software"), // unchanged
        Row(3, 300, "hr"))) // unchanged
  }

  test("merge with self subquery (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("ids") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        Seq(1, 2).toDF("value").createOrReplaceTempView("ids")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
             |USING (SELECT pk FROM $tableNameAsString r JOIN ids ON r.pk = ids.value) s
             |ON t.pk = s.pk
             |WHEN MATCHED AND t.salary = 100 THEN
             | UPDATE SET salary = t.salary + 1
             |WHEN NOT MATCHED THEN
             | INSERT (dep, salary, pk) VALUES ('new', 300, 1)
             |""".stripMargin)

        verifyJoinHint(df, Seq(JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None), JoinHint.NONE))

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 101, "hr"), // update
            Row(2, 200, "software"), // unchanged
            Row(3, 300, "hr"))) // unchanged
      }
    }
  }

  test("merge with self subquery (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("ids") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        Seq(1, 2).toDF("value").createOrReplaceTempView("ids")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
             |USING (SELECT pk FROM $tableNameAsString r JOIN ids ON r.pk = ids.value) s
             |ON t.pk = s.pk
             |WHEN MATCHED AND t.salary = 100 THEN
             | UPDATE SET salary = t.salary + 1
             |WHEN NOT MATCHED THEN
             | INSERT (dep, salary, pk) VALUES ('new', 300, 1)
             |""".stripMargin)

        verifyJoinHint(df, Seq(JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None), JoinHint.NONE))

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 101, "hr"), // update
            Row(2, 200, "software"), // unchanged
            Row(3, 300, "hr"))) // unchanged
      }
    }
  }

  test("merge with NULL values in target and source (broadcast right hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (5, None, 501, "support"),
        (6, Some(6), 601, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      val df = sql(
        s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN
           | UPDATE SET *
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      verifyJoinHint(df, JoinHint(
        Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
        None) :: Nil)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
      val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
      verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, null, 100, "hr"), // unchanged
          Row(2, 2, 200, "software"), // unchanged
          Row(5, null, 501, "support"), // insert
          Row(6, 6, 601, "support"))) // insert
    }
  }

  test("merge with NULL values in target and source (broadcast left hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (5, None, 501, "support"),
        (6, Some(6), 601, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      val df = sql(
        s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN
           | UPDATE SET *
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      verifyJoinHint(df, JoinHint(
        Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
        None) :: Nil)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
      val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
      verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, null, 100, "hr"), // unchanged
          Row(2, 2, 200, "software"), // unchanged
          Row(5, null, 501, "support"), // insert
          Row(6, 6, 601, "support"))) // insert
    }
  }

  test("merge with <=> (broadcast right hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (5, None, 501, "support"),
        (6, Some(6), 601, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      val df = sql(
        s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
           |USING source s
           |ON t.id <=> s.id
           |WHEN MATCHED THEN
           | UPDATE SET *
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      verifyJoinHint(df, JoinHint(
        Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
        None) :: Nil)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
      val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
      verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 2, 200, "software"), // unchanged
          Row(5, null, 501, "support"), // updated
          Row(6, 6, 601, "support"))) // insert
    }
  }

  test("merge with <=> (broadcast left hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (5, None, 501, "support"),
        (6, Some(6), 601, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      val df = sql(
        s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
           |USING source s
           |ON t.id <=> s.id
           |WHEN MATCHED THEN
           | UPDATE SET *
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      verifyJoinHint(df, JoinHint(
        Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
        None) :: Nil)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
      val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
      verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 2, 200, "software"), // unchanged
          Row(5, null, 501, "support"), // updated
          Row(6, 6, 601, "support"))) // insert
    }
  }

  test("merge with NULL ON condition (broadcast right hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (5, None, 501, "support"),
        (6, Some(2), 201, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      val df = sql(
        s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id AND NULL
           |WHEN MATCHED THEN
           | UPDATE SET salary = s.salary
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      verifyJoinHint(df, Seq.empty)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
      val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
      verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, null, 100, "hr"), // unchanged
          Row(2, 2, 200, "software"), // unchanged
          Row(5, null, 501, "support"), // new
          Row(6, 2, 201, "support"))) // new
    }
  }

  test("merge with NULL ON condition (broadcast left hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (5, None, 501, "support"),
        (6, Some(2), 201, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      val df = sql(
        s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id AND NULL
           |WHEN MATCHED THEN
           | UPDATE SET salary = s.salary
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      verifyJoinHint(df, Seq.empty)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
      val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
      verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, null, 100, "hr"), // unchanged
          Row(2, 2, 200, "software"), // unchanged
          Row(5, null, 501, "support"), // new
          Row(6, 2, 201, "support"))) // new
    }
  }

  test("merge with NULL clause conditions (broadcast right hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (3, 301, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      val df = sql(
        s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND NULL THEN
           | UPDATE SET salary = s.salary
           |WHEN NOT MATCHED AND NULL THEN
           | INSERT *
           |WHEN NOT MATCHED BY SOURCE AND NULL THEN
           | DELETE
           |""".stripMargin)

      verifyJoinHint(df, JoinHint(
        Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
        None) :: Nil)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
      val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
      verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // unchanged
          Row(2, 200, "software"))) // unchanged
    }
  }

  test("merge with NULL clause conditions (broadcast left hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (3, 301, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      val df = sql(
        s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND NULL THEN
           | UPDATE SET salary = s.salary
           |WHEN NOT MATCHED AND NULL THEN
           | INSERT *
           |WHEN NOT MATCHED BY SOURCE AND NULL THEN
           | DELETE
           |""".stripMargin)

      verifyJoinHint(df, JoinHint(
        Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
        None) :: Nil)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
      val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
      verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // unchanged
          Row(2, 200, "software"))) // unchanged
    }
  }

  test("merge with multiple matching clauses (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 101, "support"),
          (3, 301, "support"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED AND t.pk = 1 THEN
             | UPDATE SET salary = t.salary + 5
             |WHEN MATCHED AND t.salary = 100 THEN
             | UPDATE SET salary = t.salary + 2
             |WHEN NOT MATCHED BY SOURCE AND t.pk = 2 THEN
             | UPDATE SET salary = salary - 1
             |WHEN NOT MATCHED BY SOURCE AND t.salary = 200 THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 105, "hr"), // updated (matched)
            Row(2, 199, "software"))) // updated (not matched by source)
      }
    }
  }

  test("merge with multiple matching clauses (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // we can't apply hint due to cardinality check
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |""".stripMargin)

        val sourceRows = Seq(
          (1, 101, "support"),
          (3, 301, "support"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED AND t.pk = 1 THEN
             | UPDATE SET salary = t.salary + 5
             |WHEN MATCHED AND t.salary = 100 THEN
             | UPDATE SET salary = t.salary + 2
             |WHEN NOT MATCHED BY SOURCE AND t.pk = 2 THEN
             | UPDATE SET salary = salary - 1
             |WHEN NOT MATCHED BY SOURCE AND t.salary = 200 THEN
             | DELETE
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[WriteDeltaExec])
        val writeDeltaExec = commandResultExec.commandPhysicalPlan.asInstanceOf[WriteDeltaExec]
        verifyBroadcastHashJoinExec(writeDeltaExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 105, "hr"), // updated (matched)
            Row(2, 199, "software"))) // updated (not matched by source)
      }
    }
  }

  // calling buildReplaceDataPlan begin

  test("merge into table containing added column with default value (broadcast right hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      sql(
        s"""CREATE TABLE $tableNameAsString (
           | pk INT NOT NULL,
           | salary INT NOT NULL DEFAULT -1,
           | dep STRING)
           |PARTITIONED BY (dep)
           |""".stripMargin)

      append("pk INT NOT NULL, dep STRING",
        """{ "pk": 1, "dep": "hr" }
          |{ "pk": 2, "dep": "hr" }
          |{ "pk": 3, "dep": "hr" }
          |""".stripMargin)

      sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr", "initial-text"),
          Row(2, -1, "hr", "initial-text"),
          Row(3, -1, "hr", "initial-text")))

      val sourceRows = Seq(
        (1, 100, "hr"),
        (4, 400, "hr"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      val df = sql(
        s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET t.salary = s.salary, t.txt = DEFAULT
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, DEFAULT, s.dep)
           |WHEN NOT MATCHED BY SOURCE THEN
           | UPDATE SET salary = DEFAULT
           |""".stripMargin)

      verifyJoinHint(df, JoinHint(
        Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
        None) :: Nil)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[ReplaceDataExec])
      val replaceDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[ReplaceDataExec]
      verifyBroadcastHashJoinExec(replaceDataExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr", "initial-text"),
          Row(2, -1, "hr", "initial-text"),
          Row(3, -1, "hr", "initial-text"),
          Row(4, -1, "hr", "initial-text")))
    }
  }

  test("merge into table containing added column with default value (broadcast left hint)") {
    // we can't apply hint due to cardinality check
    withTempView("source") {
      sql(
        s"""CREATE TABLE $tableNameAsString (
           | pk INT NOT NULL,
           | salary INT NOT NULL DEFAULT -1,
           | dep STRING)
           |PARTITIONED BY (dep)
           |""".stripMargin)

      append("pk INT NOT NULL, dep STRING",
        """{ "pk": 1, "dep": "hr" }
          |{ "pk": 2, "dep": "hr" }
          |{ "pk": 3, "dep": "hr" }
          |""".stripMargin)

      sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr", "initial-text"),
          Row(2, -1, "hr", "initial-text"),
          Row(3, -1, "hr", "initial-text")))

      val sourceRows = Seq(
        (1, 100, "hr"),
        (4, 400, "hr"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      val df = sql(
        s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET t.salary = s.salary, t.txt = DEFAULT
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, DEFAULT, s.dep)
           |WHEN NOT MATCHED BY SOURCE THEN
           | UPDATE SET salary = DEFAULT
           |""".stripMargin)

      verifyJoinHint(df, JoinHint(
        Some(HintInfo(strategy = Some(NO_BROADCAST_AND_REPLICATION))),
        None) :: Nil)

      val commandResultExec = getCommandResultExec(df)
      assert(commandResultExec.commandPhysicalPlan.isInstanceOf[ReplaceDataExec])
      val replaceDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[ReplaceDataExec]
      verifyBroadcastHashJoinExec(replaceDataExec.query, 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr", "initial-text"),
          Row(2, -1, "hr", "initial-text"),
          Row(3, -1, "hr", "initial-text"),
          Row(4, -1, "hr", "initial-text")))
    }
  }

  test("merge into table containing added column with default value (matched delete action)" +
    " (broadcast right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join supports broadcast right
      withTempView("source") {
        sql(
          s"""CREATE TABLE $tableNameAsString (
             | pk INT NOT NULL,
             | salary INT NOT NULL DEFAULT -1,
             | dep STRING)
             |PARTITIONED BY (dep)
             |""".stripMargin)

        append("pk INT NOT NULL, dep STRING",
          """{ "pk": 1, "dep": "hr" }
            |{ "pk": 2, "dep": "hr" }
            |{ "pk": 3, "dep": "hr" }
            |""".stripMargin)

        sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, -1, "hr", "initial-text"),
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))

        val sourceRows = Seq(
          (1, 100, "hr"),
          (4, 400, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |WHEN NOT MATCHED BY SOURCE THEN
             | UPDATE SET salary = DEFAULT
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[ReplaceDataExec])
        val replaceDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[ReplaceDataExec]
        verifyBroadcastHashJoinExec(replaceDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))
      }
    }
  }

  test("merge into table containing added column with default value (matched delete action)" +
    " (broadcast left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join doesn't support broadcast left
      withTempView("source") {
        sql(
          s"""CREATE TABLE $tableNameAsString (
             | pk INT NOT NULL,
             | salary INT NOT NULL DEFAULT -1,
             | dep STRING)
             |PARTITIONED BY (dep)
             |""".stripMargin)

        append("pk INT NOT NULL, dep STRING",
          """{ "pk": 1, "dep": "hr" }
            |{ "pk": 2, "dep": "hr" }
            |{ "pk": 3, "dep": "hr" }
            |""".stripMargin)

        sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, -1, "hr", "initial-text"),
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))

        val sourceRows = Seq(
          (1, 100, "hr"),
          (4, 400, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ BROADCAST(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |WHEN NOT MATCHED BY SOURCE THEN
             | UPDATE SET salary = DEFAULT
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[ReplaceDataExec])
        val replaceDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[ReplaceDataExec]
        verifyBroadcastHashJoinExec(replaceDataExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))
      }
    }
  }

  test("merge into table containing added column with default value (matched delete action)" +
    " (sort merge right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        sql(
          s"""CREATE TABLE $tableNameAsString (
             | pk INT NOT NULL,
             | salary INT NOT NULL DEFAULT -1,
             | dep STRING)
             |PARTITIONED BY (dep)
             |""".stripMargin)

        append("pk INT NOT NULL, dep STRING",
          """{ "pk": 1, "dep": "hr" }
            |{ "pk": 2, "dep": "hr" }
            |{ "pk": 3, "dep": "hr" }
            |""".stripMargin)

        sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, -1, "hr", "initial-text"),
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))

        val sourceRows = Seq(
          (1, 100, "hr"),
          (4, 400, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |WHEN NOT MATCHED BY SOURCE THEN
             | UPDATE SET salary = DEFAULT
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[ReplaceDataExec])
        val replaceDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[ReplaceDataExec]
        verifySortMergeJoinExec(replaceDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))
      }
    }
  }

  test("merge into table containing added column with default value (matched delete action)" +
    " (sort merge left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        sql(
          s"""CREATE TABLE $tableNameAsString (
             | pk INT NOT NULL,
             | salary INT NOT NULL DEFAULT -1,
             | dep STRING)
             |PARTITIONED BY (dep)
             |""".stripMargin)

        append("pk INT NOT NULL, dep STRING",
          """{ "pk": 1, "dep": "hr" }
            |{ "pk": 2, "dep": "hr" }
            |{ "pk": 3, "dep": "hr" }
            |""".stripMargin)

        sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, -1, "hr", "initial-text"),
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))

        val sourceRows = Seq(
          (1, 100, "hr"),
          (4, 400, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ MERGE(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |WHEN NOT MATCHED BY SOURCE THEN
             | UPDATE SET salary = DEFAULT
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[ReplaceDataExec])
        val replaceDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[ReplaceDataExec]
        verifySortMergeJoinExec(replaceDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))
      }
    }
  }

  test("merge into table containing added column with default value (matched delete action)" +
    " (shuffle hash right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join supports shuffle hash right
      withTempView("source") {
        sql(
          s"""CREATE TABLE $tableNameAsString (
             | pk INT NOT NULL,
             | salary INT NOT NULL DEFAULT -1,
             | dep STRING)
             |PARTITIONED BY (dep)
             |""".stripMargin)

        append("pk INT NOT NULL, dep STRING",
          """{ "pk": 1, "dep": "hr" }
            |{ "pk": 2, "dep": "hr" }
            |{ "pk": 3, "dep": "hr" }
            |""".stripMargin)

        sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, -1, "hr", "initial-text"),
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))

        val sourceRows = Seq(
          (1, 100, "hr"),
          (4, 400, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_HASH(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |WHEN NOT MATCHED BY SOURCE THEN
             | UPDATE SET salary = DEFAULT
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_HASH)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[ReplaceDataExec])
        val replaceDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[ReplaceDataExec]
        verifyShuffleHashJoinExec(replaceDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))
      }
    }
  }

  test("merge into table containing added column with default value (matched delete action)" +
    " (shuffle hash left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join supports shuffle hash left
      withTempView("source") {
        sql(
          s"""CREATE TABLE $tableNameAsString (
             | pk INT NOT NULL,
             | salary INT NOT NULL DEFAULT -1,
             | dep STRING)
             |PARTITIONED BY (dep)
             |""".stripMargin)

        append("pk INT NOT NULL, dep STRING",
          """{ "pk": 1, "dep": "hr" }
            |{ "pk": 2, "dep": "hr" }
            |{ "pk": 3, "dep": "hr" }
            |""".stripMargin)

        sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, -1, "hr", "initial-text"),
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))

        val sourceRows = Seq(
          (1, 100, "hr"),
          (4, 400, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_HASH(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |WHEN NOT MATCHED BY SOURCE THEN
             | UPDATE SET salary = DEFAULT
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_HASH))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[ReplaceDataExec])
        val replaceDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[ReplaceDataExec]
        verifyShuffleHashJoinExec(replaceDataExec.query, 1)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))
      }
    }
  }

  test("merge into table containing added column with default value (matched delete action)" +
    " (cartesian product right hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join doesn't support cartesian product
      withTempView("source") {
        sql(
          s"""CREATE TABLE $tableNameAsString (
             | pk INT NOT NULL,
             | salary INT NOT NULL DEFAULT -1,
             | dep STRING)
             |PARTITIONED BY (dep)
             |""".stripMargin)

        append("pk INT NOT NULL, dep STRING",
          """{ "pk": 1, "dep": "hr" }
            |{ "pk": 2, "dep": "hr" }
            |{ "pk": 3, "dep": "hr" }
            |""".stripMargin)

        sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, -1, "hr", "initial-text"),
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))

        val sourceRows = Seq(
          (1, 100, "hr"),
          (4, 400, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(s) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |WHEN NOT MATCHED BY SOURCE THEN
             | UPDATE SET salary = DEFAULT
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL)))) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[ReplaceDataExec])
        val replaceDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[ReplaceDataExec]
        verifyCartesianProductExec(replaceDataExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))
      }
    }
  }

  test("merge into table containing added column with default value (matched delete action)" +
    " (cartesian product left hint)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The left join doesn't support cartesian product
      withTempView("source") {
        sql(
          s"""CREATE TABLE $tableNameAsString (
             | pk INT NOT NULL,
             | salary INT NOT NULL DEFAULT -1,
             | dep STRING)
             |PARTITIONED BY (dep)
             |""".stripMargin)

        append("pk INT NOT NULL, dep STRING",
          """{ "pk": 1, "dep": "hr" }
            |{ "pk": 2, "dep": "hr" }
            |{ "pk": 3, "dep": "hr" }
            |""".stripMargin)

        sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, -1, "hr", "initial-text"),
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))

        val sourceRows = Seq(
          (1, 100, "hr"),
          (4, 400, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        val df = sql(
          s"""MERGE /*+ SHUFFLE_REPLICATE_NL(t) */ INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | DELETE
             |WHEN NOT MATCHED BY SOURCE THEN
             | UPDATE SET salary = DEFAULT
             |""".stripMargin)

        verifyJoinHint(df, JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL))),
          None) :: Nil)

        val commandResultExec = getCommandResultExec(df)
        assert(commandResultExec.commandPhysicalPlan.isInstanceOf[ReplaceDataExec])
        val replaceDataExec = commandResultExec.commandPhysicalPlan.asInstanceOf[ReplaceDataExec]
        verifyCartesianProductExec(replaceDataExec.query, 0)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(2, -1, "hr", "initial-text"),
            Row(3, -1, "hr", "initial-text")))
      }
    }
  }
}
