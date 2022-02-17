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

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class BroadcastContainsJoinSuite
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    Seq[(Int, String, String)](
      (-21, null, "left-null"),
      (20, "key0", "left-0"),
      (21, "key1", "left-1"),
      (22, "key2", "left-2"))
      .toDF("id", "key", "value")
      .createOrReplaceTempView("t1")

    Seq[(Int, String, String)](
      (-31, null, "right-null"),
      (30, "key0", "right-0"),
      (31, "key1", "right-1"),
      (33, "key3", "right-3"))
      .toDF("id", "key", "value")
      .createOrReplaceTempView("t2")

    Seq[(Int, String, String)](
      (-31, null, "right-null"),
      (30, "k%0", "right-0"),
      (31, "k_y1", "right-1"),
      (33, "key3", "right-3"))
      .toDF("id", "key", "value")
      .createOrReplaceTempView("t3")
  }

  def testWithCodegen(f: => Unit): Unit = {
    Seq("false", "true").map { codegenEnabled =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled) {
        f
      }
    }
  }

  def findTopLevelBroadcastContainsJoin(plan: SparkPlan): Seq[BroadcastContainsJoinExec] = {
    collect(plan) { case j: BroadcastContainsJoinExec => j }
  }

  def findTopLevelBroadcastNestedLoopJoin(plan: SparkPlan): Seq[BroadcastNestedLoopJoinExec] = {
    collect(plan) { case j: BroadcastNestedLoopJoinExec => j }
  }

  def verifyBroadcastContainsJoinNumber(
      query: String,
      expected: Seq[Row],
      canBroadcast: Boolean,
      joinNumber: Int): Unit = testWithCodegen {
    withSQLConf(SQLConf.CONTAINS_JOIN_ENABLED.key -> "false") {
      val df = sql(query)
      checkAnswer(df, expected)
      val plan = df.queryExecution.executedPlan
      assert(findTopLevelBroadcastNestedLoopJoin(plan).size == joinNumber)
      assert(findTopLevelBroadcastContainsJoin(plan).size == 0)
    }
    withSQLConf(SQLConf.CONTAINS_JOIN_ENABLED.key -> "true") {
      val df = sql(query)
      checkAnswer(df, expected)
      val plan = df.queryExecution.executedPlan
      if (canBroadcast) {
        assert(findTopLevelBroadcastNestedLoopJoin(plan).size == 0)
        assert(findTopLevelBroadcastContainsJoin(plan).size == joinNumber)
      } else {
        assert(findTopLevelBroadcastNestedLoopJoin(plan).size == joinNumber)
        assert(findTopLevelBroadcastContainsJoin(plan).size == 0)
      }
    }
  }

  def verifyBroadcastContainsJoinNumber(
      query: String,
      expectedDF: DataFrame,
      canBroadcast: Boolean,
      joinNumber: Int): Unit = {
    verifyBroadcastContainsJoinNumber(query, expectedDF.collect(), canBroadcast, joinNumber)
  }

  test("SPARK-38238: Build token tree") {
    val dat =
      new DoubleArrayTreeBuilder[Long]()
        .compile("cat", 5)
        .compile("deep", 7)
        .compile("do", 17)
        // same key but different values
        .compile("dog", 99)
        .compile("dog", 18)
        // exactly the same value
        .compile("dogs", 21)
        .compile("dogs", 21)
        .builder()

    assert(dat.doMatch("I love beautiful girls") === ArrayBuffer())
    assert(dat.doMatch("I do very good") === ArrayBuffer(17))
    assert(dat.doMatch("A lovely dogs and a annoying dogs") === ArrayBuffer(17, 99, 18, 21, 21))
    assert(dat.doMatch("I love cats and dogs") === ArrayBuffer(5, 17, 99, 18, 21, 21))

    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    dat.writeExternal(out)
    out.flush()
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val serDeDat = new DoubleArrayTrieTree()
    serDeDat.readExternal(in)
    assert(dat == serDeDat)

    val dat2 =
      new DoubleArrayTreeBuilder[Long]()
        .compile("abcd", 1)
        .compile("bce", 2)
        .builder()

    assert(dat2.doMatch("abcdabcabcababcabcda") === ArrayBuffer(1))
  }

  test("SPARK-38238: contains join patterns") {
    verifyBroadcastContainsJoinNumber(
      s"""SELECT /*+ BROADCAST(t2) */ t1.*, t2.*
         |FROM t1 join t2
         |ON upper(t1.key) like concat('%', upper(t2.key), '%')
         |""".stripMargin,
      Seq(
        Row(20, "key0", "left-0", 30, "key0", "right-0"),
        Row(21, "key1", "left-1", 31, "key1", "right-1")),
      true,
      1)

    verifyBroadcastContainsJoinNumber(
      s"""SELECT /*+ BROADCAST(t2) */ t1.*, t2.*
         |FROM t1 join t2
         |ON position(lower(concat(t2.key, t2.value)), lower(t1.key)) > 0
         |""".stripMargin,
      Seq.empty[(Int, String, Int, String)].toDF(),
      true,
      1)

    Seq((" how old is she ", "gap"), (" how old is she ", "thorn"))
      .toDF("keyword", "source")
      .createOrReplaceTempView("thorntest")
    // scalastyle:off
    val text =
      "Thanks for getting back to me . Won’t let me either I’ll have another go tomorrow" +
        " might have to wait for package to be delivered ?? Oh my god how dreadful !!! " +
        "Very lucky it wasn’t her face . How old is she ? Bless hope it doesn’t scar" +
        " her for life .. if there’s any probs I’ll message you thanks for trying ."
    // scalastyle:on
    Seq(text)
      .toDF("msg_body_txt")
      .createOrReplaceTempView("msg")
    verifyBroadcastContainsJoinNumber(
      s"""SELECT msg_body_txt, keyword
         |FROM msg a
         |JOIN thorntest t
         |ON (POSITION(lower(t.keyword), lower(a.msg_body_txt)) > 0)
         |""".stripMargin,
      Seq(Row(text, " how old is she "), Row(text, " how old is she ")),
      true,
      1)
  }

  test("SPARK-38238: test BroadcastContainsJoin inner join") {
    // Use hint because subPlan.stats.sizeInBytes is very large.
    verifyBroadcastContainsJoinNumber(
      s"""SELECT /*+ BROADCAST(t2)*/ t1.key as key1,
         |       t1.value as value1,
         |       t2.key as key2,
         |       t2.value as value2
         |FROM t1
         |JOIN t2
         |ON t1.key like concat('%' || t2.key || '%')
         |AND length(t1.value) - length(t2.value) < 1
         |WHERE t1.value in ('left-null', 'left-0', 'left-2')
         |""".stripMargin,
      Seq(Row("key0", "left-0", "key0", "right-0")),
      true,
      1)

    verifyBroadcastContainsJoinNumber(
      s"""SELECT /*+ BROADCAST(t2)*/ t1.key as key1,
         |       t1.value as value1,
         |       t2.key as key2,
         |       t2.value as value2
         |FROM t1
         |JOIN t2
         |ON t1.key like concat('%' || t2.key || '%')
         |AND length(t1.value) - length(t2.value) > 1
         |WHERE t1.value in ('left-null', 'left-0', 'left-2')
         |""".stripMargin,
      Seq.empty[(String, String, String, String)].toDF(),
      true,
      1)

    verifyBroadcastContainsJoinNumber(
      s"""SELECT /*+ BROADCAST(t2)*/ t1.key
         |FROM t1
         |JOIN t2
         |ON t1.key like concat('%' || ' ' || t2.key || ' ' || '%')
         |WHERE t1.value in ('left-null', 'left-0', 'left-2')
         |""".stripMargin,
      Seq.empty[(String)].toDF(),
      true,
      1)
  }

  def unSupportedContainsPattern(query: String): Unit = {
    withSQLConf(
      SQLConf.CONTAINS_JOIN_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val df = sql(query)
      df.collect()
      val plan = df.queryExecution.executedPlan
      assert(findTopLevelBroadcastContainsJoin(plan).size == 0)
    }
  }

  test("SPARK-38238: extract contains pattern") {
    // Use hint because subPlan.stats.sizeInBytes is very large
    unSupportedContainsPattern(s"""SELECT /*+ BROADCAST(t2)*/ t1.key as key1,
                                  |       t1.value as value1,
                                  |       t2.key as key2,
                                  |       t2.value as value2
                                  |FROM t1
                                  |JOIN t2
                                  |ON t1.key like concat('%' || t2.key || '%' || t2.value || '%')
                                  |AND length(t1.value) - length(t2.value) < 1
                                  |WHERE t1.value in ('left-null', 'left-0', 'left-2')
                                  |""".stripMargin)
    unSupportedContainsPattern(s"""SELECT /*+ BROADCAST(t2)*/ t1.key as key1,
                                  |       t1.value as value1,
                                  |       t2.key as key2,
                                  |       t2.value as value2
                                  |FROM t1
                                  |JOIN t2
                                  |ON t1.key like concat('%' || t2.key || '_' || t2.value || '%')
                                  |AND length(t1.value) - length(t2.value) < 1
                                  |WHERE t1.value in ('left-null', 'left-0', 'left-2')
                                  |""".stripMargin)
  }

  test(
    "SPARK-38238: Fall back to NLJ if broadcast relation is in like expression " +
      "and contains % or _ is unsupported") {
    unSupportedContainsPattern(s"""SELECT /*+ BROADCAST(t3)*/ t1.key as key1,
                                  |       t1.value as value1,
                                  |       t3.key as key2,
                                  |       t3.value as value2
                                  |FROM t1
                                  |JOIN t3
                                  |ON t1.key like concat('%' || t3.key || '%')
                                  |""".stripMargin)
  }

  test("SPARK-38238: Donot fall back to NLJ for position expression") {
    verifyBroadcastContainsJoinNumber(
      s"""SELECT /*+ BROADCAST(t3)*/ t1.key as key1,
         |       t1.value as value1,
         |       t3.key as key2,
         |       t3.value as value2
         |FROM t1
         |JOIN t3
         |ON position(t3.key, t1.key) > 0
         |""".stripMargin,
      Seq.empty[(String)].toDF(),
      true,
      1)
  }

  test("SPARK-38238: test BroadcastContainsJoin outer/semi/anti join") {
    verifyBroadcastContainsJoinNumber(
      s"""SELECT *
         |FROM t1
         |LEFT JOIN t2
         |ON t1.key like concat('%' || t2.key || '%')
         |AND length(t1.value) - length(t2.value) < 1
         |WHERE t1.value in ('left-null', 'left-0', 'left-2')
         |""".stripMargin,
      Seq(
        Row(-21, null, "left-null", null, null, null),
        Row(20, "key0", "left-0", 30, "key0", "right-0"),
        Row(22, "key2", "left-2", null, null, null)),
      true,
      1)
    verifyBroadcastContainsJoinNumber(
      s"""SELECT *
         |FROM t1
         |RIGHT JOIN t2
         |ON t1.key like concat('%' || t2.key || '%')
         |AND length(t1.value) - length(t2.value) < 1
         |WHERE t2.value in ('right-null', 'right-0', 'right-3')
         |""".stripMargin,
      Seq(
        Row(null, null, null, -31, null, "right-null"),
        Row(20, "key0", "left-0", 30, "key0", "right-0"),
        Row(null, null, null, 33, "key3", "right-3")),
      false,
      1)
    verifyBroadcastContainsJoinNumber(
      s"""SELECT *
         |FROM t1
         |RIGHT JOIN t2
         |ON t2.key like concat('%' || t1.key || '%')
         |AND length(t1.value) - length(t2.value) < 1
         |WHERE t2.value in ('right-null', 'right-0', 'right-3')
         |""".stripMargin,
      Seq(
        Row(null, null, null, -31, null, "right-null"),
        Row(20, "key0", "left-0", 30, "key0", "right-0"),
        Row(null, null, null, 33, "key3", "right-3")),
      true,
      1)
    verifyBroadcastContainsJoinNumber(
      s"""SELECT /*+ BROADCAST(t2)*/ t1.*
         |FROM t1
         |SEMI JOIN t2
         |ON t1.key like concat('%' || t2.key || '%')
         |AND length(t1.value) - length(t2.value) < 1
         |WHERE t1.value in ('left-null', 'left-0', 'left-2')
         |""".stripMargin,
      Seq(Row(20, "key0", "left-0")),
      true,
      1)
    verifyBroadcastContainsJoinNumber(
      s"""SELECT /*+ BROADCAST(t2)*/ t1.*
         |FROM t1
         |ANTI JOIN t2
         |ON t1.key like concat('%' || t2.key || '%')
         |AND length(t1.value) - length(t2.value) < 1
         |WHERE t1.value in ('left-null', 'left-0', 'left-2')
         |""".stripMargin,
      Seq(Row(-21, null, "left-null"), Row(22, "key2", "left-2")),
      true,
      1)
  }
}
