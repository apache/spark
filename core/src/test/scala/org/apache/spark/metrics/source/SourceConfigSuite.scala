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

package org.apache.spark.metrics.source

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.{METRICS_EXECUTORMETRICS_SOURCE_ENABLED, METRICS_STATIC_SOURCES_ENABLED}

class SourceConfigSuite extends SparkFunSuite with LocalSparkContext {

  test("Test configuration for adding static sources registration") {
    val conf = new SparkConf()
    conf.set(METRICS_STATIC_SOURCES_ENABLED, true)
    val sc = new SparkContext("local", "test", conf)
    try {
      val metricsSystem = sc.env.metricsSystem

      // Static sources should be registered
      assert (metricsSystem.getSourcesByName("CodeGenerator").nonEmpty)
      assert (metricsSystem.getSourcesByName("HiveExternalCatalog").nonEmpty)
    } finally {
      sc.stop()
    }
  }

  test("Test configuration for skipping static sources registration") {
    val conf = new SparkConf()
    conf.set(METRICS_STATIC_SOURCES_ENABLED, false)
    val sc = new SparkContext("local", "test", conf)
    try {
      val metricsSystem = sc.env.metricsSystem

      // Static sources should not be registered
      assert (metricsSystem.getSourcesByName("CodeGenerator").isEmpty)
      assert (metricsSystem.getSourcesByName("HiveExternalCatalog").isEmpty)
    } finally {
      sc.stop()
    }
  }

  test("Test configuration for adding ExecutorMetrics source registration") {
    val conf = new SparkConf()
    conf.set(METRICS_EXECUTORMETRICS_SOURCE_ENABLED, true)
    val sc = new SparkContext("local", "test", conf)
    try {
      val metricsSystem = sc.env.metricsSystem

      // ExecutorMetrics source should be registered
      assert (metricsSystem.getSourcesByName("ExecutorMetrics").nonEmpty)
    } finally {
      sc.stop()
    }
  }

  test("Test configuration for skipping ExecutorMetrics source registration") {
    val conf = new SparkConf()
    conf.set(METRICS_EXECUTORMETRICS_SOURCE_ENABLED, false)
    val sc = new SparkContext("local", "test", conf)
    try {
      val metricsSystem = sc.env.metricsSystem

      // ExecutorMetrics source should not be registered
      assert (metricsSystem.getSourcesByName("ExecutorMetrics").isEmpty)
    } finally {
      sc.stop()
    }
  }

  test("SPARK-31711: Test executor source registration in local mode") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "test", conf)
    try {
      val metricsSystem = sc.env.metricsSystem

      // Executor source should be registered
      assert (metricsSystem.getSourcesByName("executor").nonEmpty)
    } finally {
      sc.stop()
    }
  }
}
