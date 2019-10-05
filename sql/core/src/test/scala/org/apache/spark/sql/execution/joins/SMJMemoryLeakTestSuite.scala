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

import org.apache.spark.sql.execution.SparkPlanTest
import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.test.SharedSQLContext

/**
 * This test suite permutates three sample test data, different join strategies,
 * and enable/disable spilling during UnsafeExternalSort, generates more than 400
 * test cases, in order to discover potential memory leaks, that could happen in a corner
 * combination of possibilities.
 */

class SMJMemoryLeakTestSuite extends SparkPlanTest with SharedSQLContext {

  /**
   * Calculates all permutations taking n elements of the input List,
   * with repetitions.
   * Precondition: input.length > 0 && n > 0
   */
  def permutationsWithRepetitions[T](input : List[T], n : Int) : List[List[T]] = {
    require(input.length > 0 && n > 0)
    n match {
      case 1 => for (el <- input) yield List(el)
      case _ => for (el <- input; perm <- permutationsWithRepetitions(input, n - 1)) yield el :: perm
    }
  }

  private lazy val df0 = spark.range(1, 1001).select(col("id"))
    .withColumn("value", rand()).coalesce(1)

  private lazy val df1 = spark.range(1000, 2001).select(col("id"))
    .withColumn("value", rand()).coalesce(1)

  private lazy val df2 = spark.range(1, 2001).select(col("id"))
    .withColumn("value", rand()).coalesce(1)

  private val SMJwithSortSpillingConf = Seq(
    ("spark.sql.join.preferSortMergeJoin", "true"),
    ("spark.sql.autoBroadcastJoinThreshold", "-1"),
    ("spark.sql.shuffle.partitions", "200"),
    ("spark.sql.sortMergeJoinExec.buffer.spill.threshold", "1"),
    ("spark.sql.sortMergeJoinExec.buffer.in.memory.threshold", "0")
  )

  private val SMJwithoutSortSpillingConf = Seq(
    ("spark.sql.join.preferSortMergeJoin", "true"),
    ("spark.sql.autoBroadcastJoinThreshold", "-1"),
    ("spark.sql.shuffle.partitions", "200"),
    ("spark.sql.sortMergeJoinExec.buffer.spill.threshold", "2000"),
    ("spark.sql.sortMergeJoinExec.buffer.in.memory.threshold", "2000")
  )

  {
    val list = List(0, 1, 2)
    val joinTypes = List("inner", "leftsemi")

    // Permutate data with duplicates and job strategies
    for (dataPerm <- permutationsWithRepetitions(list, 3);
         joinPerm <- permutationsWithRepetitions(joinTypes, 2)) {

      val leftIndex = dataPerm(0)
      val midIndex = dataPerm(1)
      val rightIndex = dataPerm(2)

      // Enable spilling during SMJ
      val testNameEnableSpilling = s"df$leftIndex, df$midIndex, and df$rightIndex. " +
        s"jointype1: ${joinPerm(0)} and jointype2: ${joinPerm(1)} " +
        s"spilling enabled "
      joinUtility(testNameEnableSpilling, leftIndex, midIndex, rightIndex,
        joinPerm(0), joinPerm(1), SMJwithSortSpillingConf: _*)

      // Disable spilling during SMJ
      val testNameDisableSpilling = s"df$leftIndex, df$midIndex, and df$rightIndex. " +
        s"jointype1: ${joinPerm(0)} and jointype2: ${joinPerm(1)} " +
        s"spilling disabled "
      joinUtility(testNameDisableSpilling, leftIndex, midIndex, rightIndex,
        joinPerm(0), joinPerm(1), SMJwithoutSortSpillingConf: _*)
    }
  }

  private def joinUtility( testName: String,
                           leftIndex : Int,
                           midIndex : Int,
                           rightIndex: Int,
                           joinType1: String,
                           joinType2: String,
                           sqlConf: (String, String)*) {

    // One nested SMJ when inner SMJ occurring in the right side.
    //        SMJ
    //       /   \
    //      SMJ  DF
    //     /   \
    //    DF   DF
    test(testName + " left sub tree") {
      withSQLConf(sqlConf: _*) {
        val list = Seq(df0, df1, df2)
        val joined = list(leftIndex).join(list(midIndex), Seq("id"), joinType1).coalesce(1)
        val joined2 = joined.join(list(rightIndex), Seq("id"), joinType2).coalesce(1)

        val cacheJoined = joined2.cache()
        cacheJoined.explain()
        cacheJoined.count()
      }
    }

    // One nested SMJ when inner SMJ occurring in the left side.
    //        SMJ
    //       /   \
    //      DF   SMJ
    //          /   \
    //         DF   DF
    test(testName + "right sub tree") {
      withSQLConf(sqlConf: _*) {
        val list = Seq(df0, df1, df2)
        val joined = list(midIndex).join(list(rightIndex), Seq("id"), joinType2).coalesce(1)
        val joined2 = joined.join(list(leftIndex), Seq("id"), joinType1).coalesce(1)

        val cacheJoined = joined2.cache()
        cacheJoined.explain()
        cacheJoined.count()
      }
    }
  }

  test("SPARK-21492 memory leak reproduction") {
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.shuffle.partitions", 200)
    spark.conf.set("spark.sql.codegen.wholeStage", false)

    var r1 = spark.range(1, 1001).select(col("id").alias("timestamp1"))
    r1 = r1.withColumn("value", rand())
    var r2 = spark.range(1000, 2001).select(col("id").alias("timestamp2"))
    r2 = r2.withColumn("value2", rand())
    var joined = r1.join(r2, col("timestamp1") === col("timestamp2"), "inner")
    joined = joined.coalesce(1)
    joined.explain()
    joined.count()
  }
}
