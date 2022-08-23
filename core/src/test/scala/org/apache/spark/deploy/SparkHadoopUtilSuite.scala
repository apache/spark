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

class SparkHadoopUtilSuite extends SparkFunSuite {

  /**
   * Verify that spark.hadoop options are propagated, and that
   * the default s3a options are set as expected.
   */
  test("appendSparkHadoopConfigs with propagation and defaults") {
    val sc = new SparkConf()
    val hadoopConf = new Configuration(false)
    sc.set("spark.hadoop.orc.filterPushdown", "true")
    new SparkHadoopUtil().appendSparkHadoopConfigs(sc, hadoopConf)
    assertConfigValue(hadoopConf, "orc.filterPushdown", "true" )
    assertConfigValue(hadoopConf, "fs.s3a.downgrade.syncable.exceptions", "true")
    assertConfigValue(hadoopConf, "fs.s3a.endpoint", "s3.amazonaws.com")
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
    assertConfigValue(hadoopConf, "fs.s3a.endpoint", "s3.amazonaws.com")
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
}
