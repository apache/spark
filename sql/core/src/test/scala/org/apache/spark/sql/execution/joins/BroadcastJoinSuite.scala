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

import scala.reflect.ClassTag

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{AccumulatorSuite, SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLConf, SQLContext, QueryTest}

/**
 * Test various broadcast join operators with unsafe enabled.
 *
 * Tests in this suite we need to run Spark in local-cluster mode. In particular, the use of
 * unsafe map in [[org.apache.spark.sql.execution.joins.UnsafeHashedRelation]] is not triggered
 * without serializing the hashed relation, which does not happen in local mode.
 */
class BroadcastJoinSuite extends QueryTest with BeforeAndAfterAll {
  private var sc: SparkContext = null
  private var sqlContext: SQLContext = null

  /**
   * Create a new [[SQLContext]] running in local-cluster mode with unsafe and codegen enabled.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
      .setMaster("local-cluster[2,1,1024]")
      .setAppName("testing")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
    sqlContext.setConf(SQLConf.UNSAFE_ENABLED, true)
    sqlContext.setConf(SQLConf.CODEGEN_ENABLED, true)
  }

  override def afterAll(): Unit = {
    sc.stop()
    sc = null
    sqlContext = null
  }

  /**
   * Test whether the specified broadcast join updates the peak execution memory accumulator.
   */
  private def testBroadcastJoin[T: ClassTag](name: String, joinType: String): Unit = {
    AccumulatorSuite.verifyPeakExecutionMemorySet(sc, name) {
      val df1 = sqlContext.createDataFrame(Seq((1, "4"), (2, "2"))).toDF("key", "value")
      val df2 = sqlContext.createDataFrame(Seq((1, "1"), (2, "2"))).toDF("key", "value")
      // Comparison at the end is for broadcast left semi join
      val joinExpression = df1("key") === df2("key") && df1("value") > df2("value")
      val df3 = df1.join(broadcast(df2), joinExpression, joinType)
      val plan = df3.queryExecution.executedPlan
      assert(plan.collect { case p: T => p }.size === 1)
      plan.executeCollect()
    }
  }

  test("unsafe broadcast hash join updates peak execution memory") {
    testBroadcastJoin[BroadcastHashJoin]("unsafe broadcast hash join", "inner")
  }

  test("unsafe broadcast hash outer join updates peak execution memory") {
    testBroadcastJoin[BroadcastHashOuterJoin]("unsafe broadcast hash outer join", "left_outer")
  }

  test("unsafe broadcast left semi join updates peak execution memory") {
    testBroadcastJoin[BroadcastLeftSemiJoinHash]("unsafe broadcast left semi join", "leftsemi")
  }

}
