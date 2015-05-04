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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.util.Utils
import org.scalatest.FunSuite

class VersionsSuite extends FunSuite with Logging {
  val testType = "derby"

  private def buildConf() = {
    lazy val warehousePath = Utils.createTempDir()
    lazy val metastorePath = Utils.createTempDir()
    metastorePath.delete()
    Map(
      "javax.jdo.option.ConnectionURL" -> s"jdbc:derby:;databaseName=$metastorePath;create=true",
      "hive.metastore.warehouse.dir" -> warehousePath.toString)
  }

  test("success sanity check") {
    val badClient = IsolatedClientLoader.forVersion("13", buildConf()).client
    val db = new HiveDatabase("default", "")
    badClient.createDatabase(db)
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
      val badClient = quietly { IsolatedClientLoader.forVersion("13", buildConf()).client }
    }
    assert(getNestedMessages(e) contains "Unknown column 'A0.OWNER_NAME' in 'field list'")
  }

  private val versions = Seq("12", "13")

  private var client: ClientInterface = null

  versions.foreach { version =>
    test(s"$version: listTables") {
      client = null
      client = IsolatedClientLoader.forVersion(version, buildConf()).client
      client.listTables("default")
    }

    test(s"$version: createDatabase") {
      val db = HiveDatabase("default", "")
      client.createDatabase(db)
    }

    test(s"$version: createTable") {
      val table =
        HiveTable(
          specifiedDatabase = Option("default"),
          name = "src",
          schema = Seq(HiveColumn("key", "int", "")),
          partitionColumns = Seq.empty,
          properties = Map.empty,
          serdeProperties = Map.empty,
          tableType = ManagedTable,
          location = None,
          inputFormat =
            Some(classOf[org.apache.hadoop.mapred.TextInputFormat].getName),
          outputFormat =
            Some(classOf[org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat[_, _]].getName),
          serde =
            Some(classOf[org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe].getName()))

      client.createTable(table)
    }

    test(s"$version: getTable") {
      client.getTable("default", "src")
    }
  }
}
