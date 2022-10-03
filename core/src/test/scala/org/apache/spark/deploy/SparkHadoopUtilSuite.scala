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

package org.apache.spark.deploy

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.BUFFER_SIZE

class SparkHadoopUtilSuite extends SparkFunSuite {

  private val launch = "spark launch"
  private val hadoopPropagation = "spark.hadoop propagation"
  private val hivePropagation = "spark.hive propagation"

  /**
   * Verify that spark.hadoop options are propagated, and that
   * the default s3a options are set as expected.
   */
  test("appendSparkHadoopConfigs with propagation and defaults") {
    val sc = new SparkConf()
    val hadoopConf = new Configuration(false)
    sc.set("spark.hadoop.orc.filterPushdown", "true")
    new SparkHadoopUtil().appendSparkHadoopConfigs(sc, hadoopConf)
    assertConfigMatches(hadoopConf, "orc.filterPushdown", "true", hadoopPropagation)
    assertConfigMatches(hadoopConf, "fs.s3a.downgrade.syncable.exceptions", "true", launch)
    assertConfigMatches(hadoopConf, "fs.s3a.endpoint", "s3.amazonaws.com", launch)
  }

  /**
   * An empty S3A endpoint will be overridden just as a null value
   * would.
   */

  test("appendSparkHadoopConfigs with S3A endpoint set to empty string") {
    val sc = new SparkConf()
    val hadoopConf = new Configuration(false)
    sc.set("spark.hadoop.fs.s3a.endpoint", "")
    new SparkHadoopUtil().appendSparkHadoopConfigs(sc, hadoopConf)
    assertConfigMatches(hadoopConf, "fs.s3a.endpoint", "s3.amazonaws.com", launch)
  }

  /**
   * Explicitly set the patched s3a options and verify that they are not overridden.
   */
  test("appendSparkHadoopConfigs with S3A options explicitly set") {
    val sc = new SparkConf()
    val hadoopConf = new Configuration(false)
    sc.set("spark.hadoop.fs.s3a.downgrade.syncable.exceptions", "false")
    sc.set("spark.hadoop.fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")
    new SparkHadoopUtil().appendSparkHadoopConfigs(sc, hadoopConf)
    assertConfigValue(hadoopConf, "fs.s3a.downgrade.syncable.exceptions", "false")
    assertConfigValue(hadoopConf, "fs.s3a.endpoint",
      "s3-eu-west-1.amazonaws.com")
  }

  /**
   * If the endpoint region is set (even to a blank string) in
   * "spark.hadoop.fs.s3a.endpoint.region" then the endpoint is not set,
   * even when the s3a endpoint is "".
   * This supports a feature in later hadoop versions where this configuration
   * pair triggers a revert to the "SDK to work out the region" algorithm,
   * which works on EC2 deployments.
   */
  test("appendSparkHadoopConfigs with S3A endpoint region set to an empty string") {
    val sc = new SparkConf()
    val hadoopConf = new Configuration(false)
    sc.set("spark.hadoop.fs.s3a.endpoint.region", "")
    new SparkHadoopUtil().appendSparkHadoopConfigs(sc, hadoopConf)
    // the endpoint value will not have been set
    assertConfigValue(hadoopConf, "fs.s3a.endpoint", null)
  }

  /**
   * spark.hive.* is passed to the hadoop config as hive.*.
   */
  test("spark.hive propagation") {
    val sc = new SparkConf()
    val hadoopConf = new Configuration(false)
    sc.set("spark.hive.hiveoption", "value")
    new SparkHadoopUtil().appendS3AndSparkHadoopHiveConfigurations(sc, hadoopConf)
    // the endpoint value will not have been set
    assertConfigMatches(hadoopConf, "hive.hiveoption", "value", hivePropagation)
  }

  /**
   * The explicit buffer size propagation records this.
   */
  test("buffer size propagation") {
    val sc = new SparkConf()
    val hadoopConf = new Configuration(false)
    sc.set(BUFFER_SIZE.key, "123")
    new SparkHadoopUtil().appendS3AndSparkHadoopHiveConfigurations(sc, hadoopConf)
    // the endpoint value will not have been set
    assertConfigMatches(hadoopConf, "io.file.buffer.size", "123", BUFFER_SIZE.key)
  }

  /**
   * Assert that a hadoop configuration option has the expected value.
   * @param hadoopConf configuration to query
   * @param key key to look up
   * @param expected expected value.
   */
  private def assertConfigValue(
    hadoopConf: Configuration,
    key: String,
    expected: String): Unit = {
    assert(hadoopConf.get(key) === expected,
      s"Mismatch in expected value of $key")
  }


  /**
   * Assert that a hadoop configuration option has the expected value
   * and has the expected source.
   *
   * @param hadoopConf configuration to query
   * @param key        key to look up
   * @param expected   expected value.
   * @param expectedSource expected source
   */
  private def assertConfigMatches(
    hadoopConf: Configuration,
    key: String,
    expected: String,
    expectedSource: String): Unit = {
    assertConfigValue(hadoopConf, key, expected)
    assertConfigSource(hadoopConf, key, expectedSource)
  }

  /**
   * Assert that a source of a configuration matches a specific string.
   * @param hadoopConf hadoop configuration
   * @param key key to probe
   * @param expectedSource expected source
   */
  private def assertConfigSource(
    hadoopConf: Configuration,
    key: String,
    expectedSource: String): Unit = {
    val v = hadoopConf.get(key)
    // get the possibly null source list
    val sources = hadoopConf.getPropertySources(key)
    assert(sources != null && sources.length > 0,
      s"Expected source for $key with value $v")
    assert(sources(0) ===  expectedSource,
      s"Expected source $key with value $v: to contain $expectedSource")
  }
}
