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

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.test.ConnectFunSuite
import org.apache.spark.sql.types.StructType

/**
 * Test suite that test the errors thrown when using unsupported features.
 */
class UnsupportedFeaturesSuite extends ConnectFunSuite {
  private def session = SparkSession.builder().getOrCreate()

  private def testUnsupportedFeature(name: String, errorCode: String)(
      f: SparkSession => Any): Unit = {
    test(name) {
      val e = intercept[SparkUnsupportedOperationException](f(session))
      assert(e.getCondition == "UNSUPPORTED_CONNECT_FEATURE." + errorCode)
    }
  }

  testUnsupportedFeature("SparkSession.createDataFrame(RDD)", "RDD") { session =>
    session.createDataFrame(new RDD[(Int, Int)])
  }

  testUnsupportedFeature("SparkSession.createDataFrame(RDD, StructType)", "RDD") { session =>
    val schema = new StructType().add("_1", "int").add("_2", "int")
    session.createDataFrame(new RDD[Row], schema)
  }

  testUnsupportedFeature("SparkSession.createDataFrame(JavaRDD, StructType)", "RDD") { session =>
    val schema = new StructType().add("_1", "int").add("_2", "int")
    session.createDataFrame(new JavaRDD[Row], schema)
  }

  testUnsupportedFeature("SparkSession.createDataFrame(RDD, Class)", "RDD") { session =>
    session.createDataFrame(new RDD[Int], classOf[Int])
  }

  testUnsupportedFeature("SparkSession.createDataFrame(JavaRDD, Class)", "RDD") { session =>
    session.createDataFrame(new JavaRDD[Int], classOf[Int])
  }

  testUnsupportedFeature("SparkSession.createDataset(RDD)", "RDD") { session =>
    session.createDataset(new RDD[Int])(Encoders.scalaInt)
  }

  testUnsupportedFeature("SparkSession.experimental", "SESSION_EXPERIMENTAL_METHODS") {
    _.experimental
  }

  testUnsupportedFeature("SparkSession.sessionState", "SESSION_SESSION_STATE") {
    _.sessionState
  }

  testUnsupportedFeature("SparkSession.sharedState", "SESSION_SHARED_STATE") {
    _.sharedState
  }

  testUnsupportedFeature("SparkSession.listenerManager", "SESSION_LISTENER_MANAGER") {
    _.listenerManager
  }

  testUnsupportedFeature("SparkSession.sqlContext", "SESSION_SQL_CONTEXT") {
    _.sqlContext
  }

  testUnsupportedFeature(
    "SparkSession.baseRelationToDataFrame",
    "SESSION_BASE_RELATION_TO_DATAFRAME") {
    _.baseRelationToDataFrame(new BaseRelation)
  }

  testUnsupportedFeature("SparkSession.executeCommand", "SESSION_EXECUTE_COMMAND") {
    _.executeCommand("ds", "exec", Map.empty)
  }

  testUnsupportedFeature("Dataset.queryExecution", "DATASET_QUERY_EXECUTION") {
    _.range(1).queryExecution
  }

  testUnsupportedFeature("Dataset.rdd", "RDD") {
    _.range(1).rdd
  }

  testUnsupportedFeature("Dataset.javaRDD", "RDD") {
    _.range(1).javaRDD
  }

  testUnsupportedFeature("Dataset.toJavaRDD", "RDD") {
    _.range(1).toJavaRDD
  }

  testUnsupportedFeature("DataFrameReader.json(RDD)", "RDD") {
    _.read.json(new RDD[String])
  }

  testUnsupportedFeature("DataFrameReader.json(JavaRDD)", "RDD") {
    _.read.json(new JavaRDD[String])
  }
}
