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
package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Add, Literal}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.UnionExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

class BroadcastVarHashJoinUtilsSuite extends QueryTest with BroadcastVarPushdownUtils {
  private lazy val nonPartTable1 = non_part_table1
  private lazy val nonPartTable2 = non_part_table2
  private lazy val nonPartTable3 = non_part_table3

  private lazy val partTable1 = part_table1
  private lazy val partTable2 = part_table2
  private lazy val partTable3 = part_table3

  test("test identification of batchscans for broadcast variables on simple join") {
    runWithDefaultConfig({
      val lp = nonPartTable1
        .where("c1_1".attr > 100)
        .join(nonPartTable2, Inner, Option("c1_2".attr === "c2_2".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val expectedPushDownData = Seq(
        BroadcastVarPushDownData(
          1,
          getBatchScans(plan, nonPartTable2.schema).head,
          IntegerType,
          0))
      assertPushdownData(plan, expectedPushDownData)
    })
  }

  test(
    "test identification of batchscans for broadcast variables on simple join with multi " +
      "column join") {
    runWithDefaultConfig({
      val lp = nonPartTable1
        .where("c1_1".attr > 100)
        .join(
          nonPartTable2,
          Inner,
          Option("c1_2".attr === "c2_2".attr && "c1_3".attr === "c2_3".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val targetBatchScan = getBatchScans(plan, nonPartTable2.schema).head
      val expectedPushDownData = Seq(
        BroadcastVarPushDownData(1, targetBatchScan, IntegerType, 0),
        BroadcastVarPushDownData(2, targetBatchScan, LongType, 1))
      assertPushdownData(plan, expectedPushDownData)
    })
  }

  test(
    "test identification of batchscans for broadcast variables on nested joins with single " +
      "target batch scan") {
    runWithDefaultConfig({
      val lp = nonPartTable1
        .where("c1_2".attr > 100)
        .join(
          nonPartTable2,
          Inner,
          Option("c1_1".attr === "c2_2".attr && "c1_3".attr === "c2_3".attr))
        .join(nonPartTable3.where("c3_1".attr > 500), Inner, Option("c2_1".attr === "c3_2".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val targetBatchScan = getBatchScans(plan, nonPartTable2.schema).head
      val expectedPushDownData = Seq(
        BroadcastVarPushDownData(1, targetBatchScan, IntegerType, 0),
        BroadcastVarPushDownData(2, targetBatchScan, LongType, 1),
        BroadcastVarPushDownData(0, targetBatchScan, IntegerType, 0))
      assertPushdownData(plan, expectedPushDownData)
    })
  }

  test(
    "test identification of batchscans for broadcast variables on nested joins with multi " +
      "target batch scan") {
    runWithDefaultConfig({
      val lp = nonPartTable1
        .where("c1_2".attr > 100)
        .join(
          nonPartTable2,
          Inner,
          Option("c1_1".attr === "c2_2".attr && "c1_3".attr === "c2_3".attr))
        .join(nonPartTable3.where("c3_1".attr > 500), Inner, Option("c1_1".attr === "c3_2".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val targetBatchScan1 = getBatchScans(plan, nonPartTable2.schema).head
      val targetBatchScan2 = getBatchScans(plan, nonPartTable1.schema).head
      val expectedPushDownData = Seq(
        BroadcastVarPushDownData(1, targetBatchScan1, IntegerType, 0),
        BroadcastVarPushDownData(2, targetBatchScan1, LongType, 1),
        BroadcastVarPushDownData(0, targetBatchScan2, IntegerType, 0))
      assertPushdownData(plan, expectedPushDownData)
    })
  }

  test(
    "test identification of batchscans for broadcast variables on nested joins with union " +
      "resulting in multi target batch scan") {
    runWithDefaultConfig({
      val lp = nonPartTable1
        .where("c1_2".attr > 100)
        .join(
          nonPartTable2.union(nonPartTable1).union(nonPartTable3),
          Inner,
          Option("c1_1".attr === "c2_2".attr &&
            "c1_3".attr === "c2_3".attr))
        .join(nonPartTable3.where("c3_1".attr > 500), Inner, Option("c2_1".attr === "c3_2".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val targetBatchScans = plan
        .collectFirst { case u: UnionExec =>
          u
        }
        .get
        .collectLeaves()
        .map(_.asInstanceOf[BatchScanExec])

      val expectedPushDownData = targetBatchScans.flatMap(tgBs => {
        Seq(
          BroadcastVarPushDownData(1, tgBs, IntegerType, 0),
          BroadcastVarPushDownData(2, tgBs, LongType, 1),
          BroadcastVarPushDownData(0, tgBs, IntegerType, 0))
      })
      assertPushdownData(plan, expectedPushDownData)
    })
  }

  test("test  batch scan selected because of group by on join key ") {
    runWithDefaultConfig({
      val lp = nonPartTable1
        .where("c1_1".attr > 100)
        .join(
          nonPartTable2.groupBy("c2_2".attr)("c2_2".attr, sum("c2_3".attr)),
          Inner,
          Option("c1_2".attr === "c2_2".attr))
      val plan = lp.toDF().queryExecution.sparkPlan

      val targetBs = getBatchScans(
        plan,
        StructType.apply(
          nonPartTable2.schema.filter(sf =>
            sf.name == "c2_2" || sf.name ==
              "c2_3"))).head
      val expectedPushDownData = Seq(BroadcastVarPushDownData(0, targetBs, IntegerType, 0))
      assertPushdownData(plan, expectedPushDownData)
    })
  }

  test("test no batch scan selected because of group by not on join key ") {
    runWithDefaultConfig({
      val lp = nonPartTable1
        .where("c1_1".attr > 100)
        .join(
          nonPartTable2.groupBy("c2_3".attr)("c2_3".attr, sum("c2_2".attr).as("c2_2")),
          Inner,
          Option("c1_2".attr === "c2_2".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val expectedPushDownData = Seq.empty
      assertPushdownData(plan, expectedPushDownData)
    })
  }

  test("test project node aliasing still identifies  batchscan for broadcast variables") {
    runWithPushPredRuleOff({
      val right = nonPartTable2
      val left = nonPartTable1.where("c1_1".attr < 10).select(
        "c1_1".attr.as("c11_1"),
        "c1_2".attr.as("c11_2"),
        "c1_3".attr.as("c11_3"),
        "c1_4".attr.as("c11_4"),
        "c1_5".attr.as("c11_5"))
      val lp = left.join(right, Inner, Option("c11_1".attr === "c2_1".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val expectedPushDownData = Seq(
        BroadcastVarPushDownData(
          0,
          getBatchScans(plan, non_part_table2_schema).head,
          IntegerType,
          0))
      assertPushdownData(plan, expectedPushDownData)
    })
  }

  test("test no batchscan selected as join key is not directly mapped to column in BatchScan") {
    runWithDefaultConfig({
      val lp = nonPartTable1
        .where("c1_1".attr > 100)
        .join(
          nonPartTable2.select(
            Add("c2_2".attr, Literal.apply(4)).as("c22_2"),
            "c2_1".as("c22_1"),
            "c2_3".as("c22_3"),
            "c2_4".as("c22_4"),
            "c2_5".as("c22_5")),
          Inner,
          Option("c1_2".attr === "c22_2".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val expectedPushDownData = Seq.empty
      assertPushdownData(plan, expectedPushDownData)
    })
  }

  test(
    "test identification of batchscans for broadcast variables on nested joins with single " +
      "target batch scan and same joining column in both joins") {
    runWithDefaultConfig({
      val lp = nonPartTable1
        .where("c1_2".attr > 100)
        .join(
          nonPartTable2,
          Inner,
          Option("c1_1".attr === "c2_2".attr && "c1_3".attr === "c2_3".attr))
        .join(nonPartTable3.where("c3_1".attr > 500), Inner, Option("c2_2".attr === "c3_2".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val targetBatchScan = getBatchScans(plan, nonPartTable2.schema).head
      val expectedPushDownData = Seq(
        BroadcastVarPushDownData(1, targetBatchScan, IntegerType, 0),
        BroadcastVarPushDownData(2, targetBatchScan, LongType, 1),
        BroadcastVarPushDownData(1, targetBatchScan, IntegerType, 0))
      assertPushdownData(plan, expectedPushDownData)
    })
  }

  test(
    "test identification of batchscans for broadcast variables on simple join using partition " +
      "column") {
    runWithDefaultConfig({
      val lp = partTable1
        .where("c1_1".attr > 100)
        .join(partTable2, Inner, Option("c1_2".attr === "c2_1".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val expectedPushDownData = Seq(
        BroadcastVarPushDownData(
          0,
          getBatchScans(plan, partTable2.schema).head,
          IntegerType,
          0,
          true))
      assertPushdownData(plan, expectedPushDownData)
    })

    runWithIntactDPP({
      val lp = partTable1
        .where("c1_1".attr > 100)
        .join(partTable2, Inner, Option("c1_2".attr === "c2_1".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val expectedPushDownData = Seq.empty
      assertPushdownData(plan, expectedPushDownData)
    })
  }

  test(
    "test identification of batchscans for broadcast variables on simple join with multi " +
      "column join involving partition columns") {
    runWithDefaultConfig({
      val lp = partTable1
        .where("c1_1".attr > 100)
        .join(
          partTable2,
          Inner,
          Option("c1_2".attr === "c2_1".attr && "c1_3".attr === "c2_3".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val targetBatchScan = getBatchScans(plan, partTable2.schema).head
      val expectedPushDownData = Seq(
        BroadcastVarPushDownData(0, targetBatchScan, IntegerType, 0, true),
        BroadcastVarPushDownData(2, targetBatchScan, LongType, 1))
      assertPushdownData(plan, expectedPushDownData)
    })

    runWithIntactDPP({
      val lp = partTable1
        .where("c1_1".attr > 100)
        .join(
          partTable2,
          Inner,
          Option("c1_2".attr === "c2_1".attr && "c1_3".attr === "c2_3".attr))
      val plan = lp.toDF().queryExecution.sparkPlan
      val targetBatchScan = getBatchScans(plan, partTable2.schema).head
      val expectedPushDownData = Seq(BroadcastVarPushDownData(2, targetBatchScan, LongType, 1))
      assertPushdownData(plan, expectedPushDownData)
    })
  }
}
