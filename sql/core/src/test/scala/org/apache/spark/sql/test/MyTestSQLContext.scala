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

package org.apache.spark.sql.test

import scala.language.implicitConversions

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.{DataFrame, SQLConf, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A SQLContext that can be used for local testing.
 */
private[spark] class MyLocalSQLContext(sc: SparkContext) extends SQLContext(sc) with MyTestData {

  def this() {
    this(new SparkContext("local[2]", "test-sql-context",
      new SparkConf().set("spark.sql.testkey", "true")))
  }

  // For test data
  protected override val sqlContext: SQLContext = this

  override protected[sql] def createSession(): SQLSession = {
    new this.SQLSession()
  }

  protected[sql] class SQLSession extends super.SQLSession {
    protected[sql] override lazy val conf: SQLConf = new SQLConf {
      /** Fewer partitions to speed up testing. */
      override def numShufflePartitions: Int = this.getConf(SQLConf.SHUFFLE_PARTITIONS, 5)
    }
  }

  /**
   * Turn a logical plan into a [[DataFrame]]. This should be removed once we have an easier way to
   * construct [[DataFrame]] directly out of local data without relying on implicits.
   */
  protected[sql] implicit def logicalPlanToSparkQuery(plan: LogicalPlan): DataFrame = {
    DataFrame(this, plan)
  }
}

/**
 * A scalatest trait for test suites where all tests share a single [[SQLContext]].
 */
private[spark] trait MyTestSQLContext extends SparkFunSuite with BeforeAndAfterAll {

  /**
   * The [[SQLContext]] to use for all tests in this suite.
   *
   * By default, the underlying [[SparkContext]] will be run in local mode with the default
   * test configurations.
   */
  private var _ctx: SQLContext = new MyLocalSQLContext

  /** The [[SQLContext]] to use for all tests in this suite. */
  protected def sqlContext: SQLContext = _ctx

  /**
   * The [[MyLocalSQLContext]] to use for all tests in this suite.
   * This one comes with all the data prepared in advance.
   */
  protected def sqlContextWithData: MyLocalSQLContext = {
    _ctx match {
      case local: MyLocalSQLContext => local
      case _ => fail("this SQLContext does not have data prepared in advance")
    }
  }

  /**
   * Switch to the provided [[SQLContext]].
   *
   * This stops the underlying [[SparkContext]] and expects a new one to be created.
   * This is needed because only one [[SparkContext]] is allowed per JVM.
   */
  protected def switchSQLContext(newContext: () => SQLContext): Unit = {
    if (_ctx != null) {
      _ctx.sparkContext.stop()
      _ctx = newContext()
    }
  }

  /**
   * Execute the given block of code with a custom [[SQLContext]].
   * At the end of the method, a [[MyLocalSQLContext]] will be restored.
   */
  protected def withSQLContext[T](newContext: () => SQLContext)(body: => T) {
    switchSQLContext(newContext)
    try {
      body
    } finally {
      switchSQLContext(() => new MyLocalSQLContext)
    }
  }

  protected override def afterAll(): Unit = {
    switchSQLContext(() => null)
    super.afterAll()
  }
}
