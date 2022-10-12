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

import java.net.InetAddress

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil.{SET_TO_DEFAULT_VALUES, SOURCE_SPARK_HADOOP, SOURCE_SPARK_HIVE}
import org.apache.spark.internal.config.BUFFER_SIZE

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
    assertConfigMatches(hadoopConf, "orc.filterPushdown", "true", SOURCE_SPARK_HADOOP)
    assertConfigMatches(hadoopConf, "fs.s3a.downgrade.syncable.exceptions", "true",
      SET_TO_DEFAULT_VALUES)
    assertConfigMatches(hadoopConf, "fs.s3a.endpoint", "s3.amazonaws.com", SET_TO_DEFAULT_VALUES)
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
    assertConfigMatches(hadoopConf, "fs.s3a.endpoint", "s3.amazonaws.com", SET_TO_DEFAULT_VALUES)
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
   * This supports a feature in hadoop 3.3.1 where this configuration
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
  test("SPARK-40640: spark.hive propagation") {
    val sc = new SparkConf()
    val hadoopConf = new Configuration(false)
    sc.set("spark.hive.hiveoption", "value")
    new SparkHadoopUtil().appendS3AndSparkHadoopHiveConfigurations(sc, hadoopConf)
    assertConfigMatches(hadoopConf, "hive.hiveoption", "value", SOURCE_SPARK_HIVE)
  }

  /**
   * The explicit buffer size propagation records this.
   */
  test("SPARK-40640: buffer size propagation") {
    val sc = new SparkConf()
    val hadoopConf = new Configuration(false)
    sc.set(BUFFER_SIZE.key, "123")
    new SparkHadoopUtil().appendS3AndSparkHadoopHiveConfigurations(sc, hadoopConf)
    assertConfigMatches(hadoopConf, "io.file.buffer.size", "123", BUFFER_SIZE.key)
  }

  test("SPARK-40640: aws credentials from environment variables") {
    val hadoopConf = new Configuration(false)
    SparkHadoopUtil.appendS3CredentialsFromEnvironment(hadoopConf,
      "access-key", "secret-key", "session-token")
    val source = "Set by Spark on " + InetAddress.getLocalHost + " from "
    assertConfigMatches(hadoopConf, "fs.s3a.access.key", "access-key", source)
    assertConfigMatches(hadoopConf, "fs.s3a.secret.key", "secret-key", source)
    assertConfigMatches(hadoopConf, "fs.s3a.session.token", "session-token", source)
  }

  test("SPARK-19739: S3 session token propagation requires access and secret keys") {
    val hadoopConf = new Configuration(false)
    SparkHadoopUtil.appendS3CredentialsFromEnvironment(hadoopConf, null, null, "session-token")
    assertConfigValue(hadoopConf, "fs.s3a.session.token", null)
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
   * @param expectedSource string required to be in the property source string
   */
  private def assertConfigMatches(
      hadoopConf: Configuration,
      key: String,
      expected: String,
      expectedSource: String): Unit = {
    assertConfigValue(hadoopConf, key, expected)
    assertConfigSourceContains(hadoopConf, key, expectedSource)
  }

  /**
   * Assert that a source of a configuration matches a specific string.
   * @param hadoopConf hadoop configuration
   * @param key key to probe
   * @param expectedSource expected source
   */
  private def assertConfigSourceContains(
      hadoopConf: Configuration,
      key: String,
      expectedSource: String): Unit = {
    val v = hadoopConf.get(key)
    // get the source list
    val origin = SparkHadoopUtil.propertySources(hadoopConf, key)
    assert(origin.nonEmpty, s"Sources are missing for '$key' with value '$v'")
    assert(origin.contains(expectedSource),
      s"Expected source $key with value $v: and source $origin to contain $expectedSource")
  }
}
