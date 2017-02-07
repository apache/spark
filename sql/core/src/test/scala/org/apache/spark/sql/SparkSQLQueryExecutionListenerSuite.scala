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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.mock.MockitoSugar

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.{OutputParams, QueryExecutionListener}

/**
 * Test cases for the property 'spark.sql.queryExecutionListeners' that adds the
 * @see `QueryExecutionListener` to a @see `SparkSession`
 */
class SparkSQLQueryExecutionListenerSuite
    extends SparkFunSuite
    with MockitoSugar
    with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SparkContext.clearActiveContext()
  }

  test("Creation of SparkContext with non-existent QueryExecutionListener class fails fast") {
    intercept[ClassNotFoundException] {
      SparkSession
        .builder()
        .master("local")
        .config("spark.sql.queryExecutionListeners", "non.existent.QueryExecutionListener")
        .getOrCreate()
    }
    assert(!SparkSession.getDefaultSession.isDefined)
  }

  test("QueryExecutionListener that doesn't have a default constructor fails fast") {
    intercept[InstantiationException] {
      SparkSession
        .builder()
        .master("local")
        .config("spark.sql.queryExecutionListeners", classOf[NoZeroArgConstructorListener].getName)
        .getOrCreate()
    }
    assert(!SparkSession.getDefaultSession.isDefined)
  }

  test("Normal QueryExecutionListeners gets added as listeners") {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .config("mykey", "myvalue")
      .config("spark.sql.queryExecutionListeners",
        classOf[NormalQueryExecutionListener].getName + " ,"
          + classOf[AnotherQueryExecutionListener].getName)
      .getOrCreate()
    assert(SparkSession.getDefaultSession.isDefined)
    assert(NormalQueryExecutionListener.successCount === 0)
    assert(NormalQueryExecutionListener.failureCount === 0)
    assert(AnotherQueryExecutionListener.successCount === 0)
    assert(AnotherQueryExecutionListener.failureCount === 0)
    sparkSession.listenerManager.onSuccess("test1", mock[QueryExecution], 0)
    assert(NormalQueryExecutionListener.successCount === 1)
    assert(NormalQueryExecutionListener.failureCount === 0)
    assert(AnotherQueryExecutionListener.successCount === 1)
    assert(AnotherQueryExecutionListener.failureCount === 0)
    sparkSession.listenerManager.onFailure("test2", mock[QueryExecution], new Exception)
    assert(NormalQueryExecutionListener.successCount === 1)
    assert(NormalQueryExecutionListener.failureCount === 1)
    assert(AnotherQueryExecutionListener.successCount === 1)
    assert(AnotherQueryExecutionListener.failureCount === 1)
  }
}

class NoZeroArgConstructorListener(myString: String) extends QueryExecutionListener {

  override def onSuccess(
      funcName: String,
      qe: QueryExecution,
      durationNs: Long,
      options: Option[OutputParams]
  ): Unit = {}

  override def onFailure(
      funcName: String,
      qe: QueryExecution,
      exception: Exception,
      options: Option[OutputParams]
  ): Unit = {}
}

class NormalQueryExecutionListener extends QueryExecutionListener {

  override def onSuccess(
      funcName: String,
      qe: QueryExecution,
      durationNs: Long,
      options: Option[OutputParams]
  ): Unit = { NormalQueryExecutionListener.successCount += 1 }

  override def onFailure(
      funcName: String,
      qe: QueryExecution,
      exception: Exception,
      options: Option[OutputParams]
  ): Unit = { NormalQueryExecutionListener.failureCount += 1 }
}

object NormalQueryExecutionListener {
  var successCount = 0;
  var failureCount = 0;
}

class AnotherQueryExecutionListener extends QueryExecutionListener {

  override def onSuccess(
      funcName: String,
      qe: QueryExecution,
      durationNs: Long,
      options: Option[OutputParams]
  ): Unit = { AnotherQueryExecutionListener.successCount += 1 }

  override def onFailure(
      funcName: String,
      qe: QueryExecution,
      exception: Exception,
      options: Option[OutputParams]
  ): Unit = { AnotherQueryExecutionListener.failureCount += 1 }
}

object AnotherQueryExecutionListener {
  var successCount = 0;
  var failureCount = 0;
}
