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

package org.apache.spark.sql.execution.datasources.orc

import java.lang.invoke.MethodHandles
import java.util.{Map => JMap}
import java.util.Random

import scala.collection.mutable

import org.apache.orc.impl.{CryptoUtils, HadoopShimsFactory, KeyProvider}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class OrcEncryptionSuite extends OrcTest with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.hadoop.hadoop.security.key.provider.path", "test:///")
  }

  override def beforeAll(): Unit = {
    // Backup `CryptoUtils#keyProviderCache` and clear it.
    keyProviderCacheRef.entrySet()
      .forEach(e => keyProviderCacheBackup.put(e.getKey, e.getValue))
    keyProviderCacheRef.clear()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    // Restore `CryptoUtils#keyProviderCache`.
    keyProviderCacheRef.clear()
    keyProviderCacheBackup.foreach { case (k, v) => keyProviderCacheRef.put(k, v) }
  }

  val originalData = Seq(("123456789", "dongjoon@apache.org", "Dongjoon Hyun"))
  val rowDataWithoutKey =
    Row(null, "841626795E7D351555B835A002E3BF10669DE9B81C95A3D59E10865AC37EA7C3", "Dongjoon Hyun")

  private val keyProviderCacheBackup: mutable.Map[String, KeyProvider] = mutable.Map.empty

  private val keyProviderCacheRef: JMap[String, KeyProvider] = {
    val clazz = classOf[CryptoUtils]
    val lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup())
    lookup.findStaticVarHandle(clazz, "keyProviderCache", classOf[JMap[_, _]])
      .get().asInstanceOf[JMap[String, KeyProvider]]
  }

  test("Write and read an encrypted file") {
    val conf = spark.sessionState.newHadoopConf()
    val provider = HadoopShimsFactory.get.getHadoopKeyProvider(conf, new Random)
    assume(!provider.getKeyNames.isEmpty,
      s"$provider doesn't has the test keys. ORC shim is created with old Hadoop libraries")

    val df = originalData.toDF("ssn", "email", "name")

    withTempPath { dir =>
      val path = dir.getAbsolutePath
      withSQLConf(
        "hadoop.security.key.provider.path" -> "test:///",
        "orc.key.provider" -> "hadoop",
        "orc.encrypt" -> "pii:ssn,email",
        "orc.mask" -> "nullify:ssn;sha256:email") {
        df.write.mode("overwrite").orc(path)
        checkAnswer(spark.read.orc(path), df)
      }

      withSQLConf(
        "orc.key.provider" -> "memory",
        "orc.encrypt" -> "pii:ssn,email",
        "orc.mask" -> "nullify:ssn;sha256:email") {
        checkAnswer(spark.read.orc(path), rowDataWithoutKey)
      }
    }
  }

  test("Write and read an encrypted table") {
    val conf = spark.sessionState.newHadoopConf()
    val provider = HadoopShimsFactory.get.getHadoopKeyProvider(conf, new Random)
    assume(!provider.getKeyNames.isEmpty,
      s"$provider doesn't has the test keys. ORC shim is created with old Hadoop libraries")

    val df = originalData.toDF("ssn", "email", "name")

    withTempDir { dir =>
      val path = dir.getAbsolutePath
      withTable("encrypted") {
        sql(
          s"""
            |CREATE TABLE encrypted (
            |  ssn STRING,
            |  email STRING,
            |  name STRING
            |)
            |USING ORC
            |LOCATION "$path"
            |OPTIONS (
            |  hadoop.security.key.provider.path "test:///",
            |  orc.key.provider "hadoop",
            |  orc.encrypt "pii:ssn,email",
            |  orc.mask "nullify:ssn;sha256:email"
            |)
            |""".stripMargin)
        sql("INSERT INTO encrypted VALUES('123456789', 'dongjoon@apache.org', 'Dongjoon Hyun')")
        checkAnswer(sql("SELECT * FROM encrypted"), df)
      }
      withTable("normal") {
        sql(
          s"""
            |CREATE TABLE normal (
            |  ssn STRING,
            |  email STRING,
            |  name STRING
            |)
            |USING ORC
            |LOCATION "$path"
            |OPTIONS (
            |  orc.key.provider "memory",
            |  orc.encrypt "pii:ssn,email",
            |  orc.mask "nullify:ssn;sha256:email"
            |)
            |""".stripMargin)
        checkAnswer(sql("SELECT * FROM normal"), rowDataWithoutKey)
      }
    }
  }

  test("SPARK-35325: Write and read encrypted nested columns") {
    val conf = spark.sessionState.newHadoopConf()
    val provider = HadoopShimsFactory.get.getHadoopKeyProvider(conf, new Random)
    assume(!provider.getKeyNames.isEmpty,
      s"$provider doesn't has the test keys. ORC shim is created with old Hadoop libraries")

    val originalNestedData = Row(1, Row("123456789", "dongjoon@apache.org", "Dongjoon"))
    val rowNestedDataWithoutKey =
      Row(1, Row(null, "841626795E7D351555B835A002E3BF10669DE9B81C95A3D59E10865AC37EA7C3",
        "Dongjoon"))

    withTempDir { dir =>
      val path = dir.getAbsolutePath
      withTable("encrypted") {
        sql(
          s"""
            |CREATE TABLE encrypted (
            |  id INT,
            |  contact struct<ssn:STRING, email:STRING, name:STRING>
            |)
            |USING ORC
            |LOCATION "$path"
            |OPTIONS (
            |  hadoop.security.key.provider.path "test:///",
            |  orc.key.provider "hadoop",
            |  orc.encrypt "pii:contact.ssn,contact.email",
            |  orc.mask "nullify:contact.ssn;sha256:contact.email"
            |)
            |""".stripMargin)
        sql("INSERT INTO encrypted VALUES(1, ('123456789', 'dongjoon@apache.org', 'Dongjoon'))")
        checkAnswer(sql("SELECT * FROM encrypted"), originalNestedData)
      }
      withTable("normal") {
        sql(
          s"""
            |CREATE TABLE normal (
            |  id INT,
            |  contact struct<ssn:STRING, email:STRING, name:STRING>
            |)
            |USING ORC
            |LOCATION "$path"
            |OPTIONS (
            |  orc.key.provider "memory"
            |)
            |""".stripMargin)
        checkAnswer(sql("SELECT * FROM normal"), rowNestedDataWithoutKey)
      }
    }
  }

  test("SPARK-35992: Write and read fully-encrypted columns with default masking") {
    val conf = spark.sessionState.newHadoopConf()
    val provider = HadoopShimsFactory.get.getHadoopKeyProvider(conf, new Random)
    assume(!provider.getKeyNames.isEmpty,
      s"$provider doesn't has the test keys. ORC shim is created with old Hadoop libraries")

    val df = originalData.toDF("ssn", "email", "name")

    withTempPath { dir =>
      val path = dir.getAbsolutePath
      withSQLConf(
        "hadoop.security.key.provider.path" -> "test:///",
        "orc.key.provider" -> "hadoop",
        "orc.encrypt" -> "pii:ssn,email,name") {
        df.write.mode("overwrite").orc(path)
        checkAnswer(spark.read.orc(path), df)
      }

      withSQLConf(
        "orc.key.provider" -> "memory",
        "orc.encrypt" -> "pii:ssn,email,name") {
        checkAnswer(spark.read.orc(path), Row(null, null, null))
      }
    }

    val originalNestedData = Row(1, Row("123456789", "dongjoon@apache.org", "Dongjoon"))

    withTempDir { dir =>
      val path = dir.getAbsolutePath
      withTable("encrypted") {
        sql(
          s"""
            |CREATE TABLE encrypted (
            |  id INT,
            |  contact struct<ssn:STRING, email:STRING, name:STRING>
            |)
            |USING ORC
            |LOCATION "$path"
            |OPTIONS (
            |  hadoop.security.key.provider.path "test:///",
            |  orc.key.provider "hadoop",
            |  orc.encrypt "pii:id,contact"
            |)
            |""".stripMargin)
        sql("INSERT INTO encrypted VALUES(1, ('123456789', 'dongjoon@apache.org', 'Dongjoon'))")
        checkAnswer(sql("SELECT * FROM encrypted"), originalNestedData)
      }
      withTable("normal") {
        sql(
          s"""
            |CREATE TABLE normal (
            |  id INT,
            |  contact struct<ssn:STRING, email:STRING, name:STRING>
            |)
            |USING ORC
            |LOCATION "$path"
            |OPTIONS (
            |  orc.key.provider "memory"
            |)
            |""".stripMargin)
        checkAnswer(sql("SELECT * FROM normal"), Row(null, null))
        checkAnswer(sql("SELECT id, contact.* FROM normal"), Row(null, null, null, null))
      }
    }
  }
}
