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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.scalatest.Suite

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.tags.{ExtendedHiveTest, SlowHiveTest}

/**
 * A simple set of tests that call the methods of a [[HiveClient]], loading different version
 * of hive from maven central.  These tests are simple in that they are mostly just testing to make
 * sure that reflective calls are not throwing NoSuchMethod error, but the actually functionality
 * is not fully tested.
 */
@SlowHiveTest
@ExtendedHiveTest
class HiveClientSuites extends SparkFunSuite with HiveClientVersions {

  override protected val enableAutoThreadAudit = false

  import HiveClientBuilder.buildClient

  test("success sanity check") {
    val badClient = buildClient(HiveUtils.builtinHiveVersion, new Configuration())
    val db = CatalogDatabase("default", "desc", new URI("loc"), Map())
    badClient.createDatabase(db, ignoreIfExists = true)
  }

  test("hadoop configuration preserved") {
    val hadoopConf = new Configuration()
    hadoopConf.set("test", "success")
    val client = buildClient(HiveUtils.builtinHiveVersion, hadoopConf)
    assert("success" === client.getConf("test", null))
  }

  test("override useless and side-effect hive configurations") {
    Seq("spark", "tez").foreach { hiveExecEngine =>
      val hadoopConf = new Configuration()
      // These hive flags should be reset by spark
      hadoopConf.setBoolean("hive.cbo.enable", true)
      hadoopConf.setBoolean("hive.session.history.enabled", true)
      hadoopConf.set("hive.execution.engine", hiveExecEngine)
      val client = buildClient(HiveUtils.builtinHiveVersion, hadoopConf)
      assert(!client.getConf("hive.cbo.enable", "true").toBoolean)
      assert(!client.getConf("hive.session.history.enabled", "true").toBoolean)
      assert(client.getConf("hive.execution.engine", hiveExecEngine) === "mr")
    }
  }

  private def getNestedMessages(e: Throwable): String = {
    var causes = ""
    var lastException = e
    while (lastException != null) {
      causes += lastException.toString + "\n"
      lastException = lastException.getCause
    }
    causes
  }

  // Its actually pretty easy to mess things up and have all of your tests "pass" by accidentally
  // connecting to an auto-populated, in-process metastore.  Let's make sure we are getting the
  // versions right by forcing a known compatibility failure.
  // TODO: currently only works on mysql where we manually create the schema...
  ignore("failure sanity check") {
    val e = intercept[Throwable] {
      val badClient = quietly { buildClient("13", new Configuration()) }
    }
    assert(getNestedMessages(e) contains "Unknown column 'A0.OWNER_NAME' in 'field list'")
  }

  override def nestedSuites: IndexedSeq[Suite] = {
    versions.map(new HiveClientSuite(_))
  }
}
