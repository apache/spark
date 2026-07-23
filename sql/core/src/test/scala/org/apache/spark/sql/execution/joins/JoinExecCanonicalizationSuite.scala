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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SparkPlanTest
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

class JoinExecCanonicalizationSuite  extends SparkPlanTest with SharedSparkSession {
  private lazy val left = {
    spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(1, 1.0, "a"),
        Row(2, 2.0, "b" ),
        Row(3, 3.0, "c" )
      )),
      new StructType().add("a1", IntegerType).add("b1", DoubleType).add("c1", StringType))
      .createOrReplaceTempView("table1")
    spark.table("table1")
  }

  private lazy val right = {
    spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(1, 1.0, "a"),
        Row(2, 2.0, "b"),
        Row(3, 3.0, "c")
      )),
      new StructType().add("a2", IntegerType).add("b2", DoubleType).add("c2", StringType))
        .createOrReplaceTempView("table2")
    spark.table("table2")
  }

  private lazy val joinExpression1 = left("a1") === right("a2") && left("b1") === right("b2") &&
    left("c1") === right("c2")

  private lazy val joinExpression2 = left("c1") === right("c2") && left("a1") === right("a2") &&
    left("b1") === right("b2")

  test("SPARK-57127: BroadcastHashJoinExec Canonicalization") {
     val join1 = left.join(broadcast(right), joinExpression1, "inner").queryExecution.sparkPlan
    val join2 = left.join(broadcast(right), joinExpression2, "inner").queryExecution.sparkPlan
    assert(join1.isInstanceOf[BroadcastHashJoinExec])
    assert(join2.isInstanceOf[BroadcastHashJoinExec])
    assert(join1.sameResult(join2))
  }

  test("SPARK-57127: ShuffledHashJoinExec Canonicalization") {
    val join1 = left.hint("shuffle_hash").join(right, joinExpression1, "inner").queryExecution
      .sparkPlan
    val join2 = left.hint("shuffle_hash").join(right, joinExpression2, "inner").queryExecution
      .sparkPlan
    assert(join1.isInstanceOf[ShuffledHashJoinExec])
    assert(join2.isInstanceOf[ShuffledHashJoinExec])
    assert(join1.sameResult(join2))
  }

  test("SPARK-57127: SortMergeJoinExec Canonicalization") {
    val join1 = left.hint("merge").join(right, joinExpression1, "inner").queryExecution
      .sparkPlan
    val join2 = left.hint("merge").join(right, joinExpression2, "inner").queryExecution
      .sparkPlan
    assert(join1.isInstanceOf[SortMergeJoinExec])
    assert(join2.isInstanceOf[SortMergeJoinExec])
    assert(join1.sameResult(join2))
  }
}
