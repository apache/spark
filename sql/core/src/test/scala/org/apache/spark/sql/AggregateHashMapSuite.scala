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

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf

class SingleLevelAggregateHashMapSuite extends DataFrameAggregateSuite with BeforeAndAfter {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.codegen.fallback", "false")
    .set("spark.sql.codegen.aggregate.map.twolevel.enable", "false")

  // adding some checking after each test is run, assuring that the configs are not changed
  // in test code
  after {
    assert(sparkConf.get("spark.sql.codegen.fallback") == "false",
      "configuration parameter changed in test body")
    assert(sparkConf.get("spark.sql.codegen.aggregate.map.twolevel.enable") == "false",
      "configuration parameter changed in test body")
  }
}

class TwoLevelAggregateHashMapSuite extends DataFrameAggregateSuite with BeforeAndAfter {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.codegen.fallback", "false")
    .set("spark.sql.codegen.aggregate.map.twolevel.enable", "true")

  // adding some checking after each test is run, assuring that the configs are not changed
  // in test code
  after {
    assert(sparkConf.get("spark.sql.codegen.fallback") == "false",
      "configuration parameter changed in test body")
    assert(sparkConf.get("spark.sql.codegen.aggregate.map.twolevel.enable") == "true",
      "configuration parameter changed in test body")
  }
}

class TwoLevelAggregateHashMapWithVectorizedMapSuite
  extends DataFrameAggregateSuite
  with BeforeAndAfter {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.codegen.fallback", "false")
    .set("spark.sql.codegen.aggregate.map.twolevel.enable", "true")
    .set("spark.sql.codegen.aggregate.map.vectorized.enable", "true")

  // adding some checking after each test is run, assuring that the configs are not changed
  // in test code
  after {
    assert(sparkConf.get("spark.sql.codegen.fallback") == "false",
      "configuration parameter changed in test body")
    assert(sparkConf.get("spark.sql.codegen.aggregate.map.twolevel.enable") == "true",
      "configuration parameter changed in test body")
    assert(sparkConf.get("spark.sql.codegen.aggregate.map.vectorized.enable") == "true",
      "configuration parameter changed in test body")
  }
}

