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
  }

  /**
   * Explicitly set the patched s3a options and verify that they are not overridden.
   */
  test("appendSparkHadoopConfigs with S3A options explicitly set") {
    val sc = new SparkConf()
    val hadoopConf = new Configuration(false)
    sc.set("spark.hadoop.fs.s3a.downgrade.syncable.exceptions", "false")
    new SparkHadoopUtil().appendSparkHadoopConfigs(sc, hadoopConf)
    assertConfigValue(hadoopConf, "fs.s3a.downgrade.syncable.exceptions", "false")
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
      "endpoint", "access-key", "secret-key", "session-token")
    val source = "Set by Spark on " + InetAddress.getLocalHost + " from "
    assertConfigMatches(hadoopConf, "fs.s3a.endpoint", "endpoint", source)
    assertConfigMatches(hadoopConf, "fs.s3a.access.key", "access-key", source)
    assertConfigMatches(hadoopConf, "fs.s3a.secret.key", "secret-key", source)
    assertConfigMatches(hadoopConf, "fs.s3a.session.token", "session-token", source)
  }

  test("SPARK-19739: S3 session token propagation requires access and secret keys") {
    val hadoopConf = new Configuration(false)
    SparkHadoopUtil.appendS3CredentialsFromEnvironment(
      hadoopConf, null, null, null, "session-token")
    assertConfigValue(hadoopConf, "fs.s3a.session.token", null)
  }

  test("SPARK-45404: aws endpoint propagation requires access and secret keys") {
    val hadoopConf = new Configuration(false)
    SparkHadoopUtil.appendS3CredentialsFromEnvironment(
      hadoopConf, "endpoint", null, null, null)
    assertConfigValue(hadoopConf, "fs.s3a.endpoint", null)
  }

  test("substituteHadoopVariables") {
    val hadoopConf = new Configuration(false)
    hadoopConf.set("xxx", "yyy")

    val text1 = "${hadoopconf-xxx}"
    val result1 = new SparkHadoopUtil().substituteHadoopVariables(text1, hadoopConf)
    assert(result1 == "yyy")

    val text2 = "${hadoopconf-xxx"
    val result2 = new SparkHadoopUtil().substituteHadoopVariables(text2, hadoopConf)
    assert(result2 == "${hadoopconf-xxx")

    val text3 = "${hadoopconf-xxx}zzz"
    val result3 = new SparkHadoopUtil().substituteHadoopVariables(text3, hadoopConf)
    assert(result3 == "yyyzzz")

    val text4 = "www${hadoopconf-xxx}zzz"
    val result4 = new SparkHadoopUtil().substituteHadoopVariables(text4, hadoopConf)
    assert(result4 == "wwwyyyzzz")

    val text5 = "www${hadoopconf-xxx}"
    val result5 = new SparkHadoopUtil().substituteHadoopVariables(text5, hadoopConf)
    assert(result5 == "wwwyyy")

    val text6 = "www${hadoopconf-xxx"
    val result6 = new SparkHadoopUtil().substituteHadoopVariables(text6, hadoopConf)
    assert(result6 == "www${hadoopconf-xxx")

    val text7 = "www$hadoopconf-xxx}"
    val result7 = new SparkHadoopUtil().substituteHadoopVariables(text7, hadoopConf)
    assert(result7 == "www$hadoopconf-xxx}")

    val text8 = "www{hadoopconf-xxx}"
    val result8 = new SparkHadoopUtil().substituteHadoopVariables(text8, hadoopConf)
    assert(result8 == "www{hadoopconf-xxx}")
  }

  test("Redundant character escape '\\}' in RegExp ") {
    val HADOOP_CONF_PATTERN_1 = "(\\$\\{hadoopconf-[^}$\\s]+})".r.unanchored
    val HADOOP_CONF_PATTERN_2 = "(\\$\\{hadoopconf-[^}$\\s]+\\})".r.unanchored

    val text = "www${hadoopconf-xxx}zzz"
    val target1 = text match {
      case HADOOP_CONF_PATTERN_1(matched) => text.replace(matched, "yyy")
    }
    val target2 = text match {
      case HADOOP_CONF_PATTERN_2(matched) => text.replace(matched, "yyy")
    }
    assert(target1 == "wwwyyyzzz")
    assert(target2 == "wwwyyyzzz")
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
