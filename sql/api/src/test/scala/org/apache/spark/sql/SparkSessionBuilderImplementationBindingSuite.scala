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

// scalastyle:off funsuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{max, sum}

/**
 * Test suite for SparkSession implementation binding.
 */
trait SparkSessionBuilderImplementationBindingSuite
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
// scalastyle:on

  protected def sparkContext: SparkContext
  protected def implementationPackageName: String = getClass.getPackageName

  private def assertInCorrectPackage[T](obj: T): Unit = {
    assert(obj.getClass.getPackageName == implementationPackageName)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    clearSessions()
  }

  override protected def afterAll(): Unit = {
    clearSessions()
    super.afterAll()
  }

  private def clearSessions(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  test("range") {
    val session = SparkSession.builder().getOrCreate()
    assertInCorrectPackage(session)
    import session.implicits._
    val df = session.range(10).agg(sum("id")).as[Long]
    assert(df.head() == 45)
  }

  test("sqlContext") {
    SparkSession.clearActiveSession()
    val ctx = SQLContext.getOrCreate(sparkContext)
    assertInCorrectPackage(ctx)
    import ctx.implicits._
    val df = ctx.createDataset(1 to 11).select(max("value").as[Long])
    assert(df.head() == 11)
  }
}
