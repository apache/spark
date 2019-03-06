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

package org.apache.spark.sql.hive.client

import scala.language.existentials

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.tags.ExtendedHiveTest
import org.apache.spark.util.Utils

/**
 * A separate test that uses Hive 3.1 libraries, which behave a little differently
 * from the built-in ones.
 */
@ExtendedHiveTest
class Hive_3_1_ClientSuite extends SparkFunSuite with TestHiveSingleton {

  private var catalog = {
    val warehouse = Utils.createTempDir()
    val metastore = Utils.createTempDir()
    metastore.delete()
    val sparkConf = new SparkConf()
      .set(SparkLauncher.SPARK_MASTER, "local")
      .set(UI_ENABLED, false)
      .set(WAREHOUSE_PATH.key, warehouse.toURI().toString())
      .set(CATALOG_IMPLEMENTATION.key, "hive")
      .set(HiveUtils.HIVE_METASTORE_VERSION.key, "3.1")
      .set(HiveUtils.HIVE_METASTORE_JARS.key, "maven")

    val hadoopConf = new Configuration()
    hadoopConf.set("hive.metastore.warehouse.dir", warehouse.toURI().toString())
    hadoopConf.set("javax.jdo.option.ConnectionURL",
      s"jdbc:derby:;databaseName=${metastore.getAbsolutePath()};create=true")
    // These options are needed since the defaults in Hive 3.1 cause exceptions with an
    // empty metastore db.
    hadoopConf.set("datanucleus.schema.autoCreateAll", "true")
    hadoopConf.set("hive.metastore.schema.verification", "false")
    hadoopConf.set("hive.in.test", "true")

    new HiveExternalCatalog(sparkConf, hadoopConf)
  }

  override def afterAll(): Unit = {
    try {
      catalog = null
    } finally {
      super.afterAll()
    }
  }

  test("Hive 3.1.1 does not fully support runSqlHive") {
    // HIVE-17626(Hive 3.0.0) add ReExecDriver
    val e1 = intercept[AnalysisException] {
      catalog.client.runSqlHive("create table t1(c1 int)")
    }.getMessage
    assert(e1.contains(
      "Dose not support Hive 3.1.1 processor: org.apache.hadoop.hive.ql.reexec.ReExecDriver"))

    catalog.client.runSqlHive("set hive.query.reexecution.enabled=false")

    // HIVE-18238(Hive 3.0.0) changed the close() function return type.
    // This change is not compatible with the built-in Hive.
    val e2 = intercept[AnalysisException] {
      catalog.client.runSqlHive("create table t2(c1 int)")
    }.getMessage
    assert(e2.contains(
      "Dose not support Hive 3.1.1 processor: org.apache.hadoop.hive.ql.Driver"))
  }

}
