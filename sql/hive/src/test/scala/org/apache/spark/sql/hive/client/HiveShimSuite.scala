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

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.ql.metadata.{Hive, Table}
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.hive.client.HiveClientImpl.getHive
import org.apache.spark.util.Utils

class HiveShimSuite(version: String) extends HiveVersionSuite(version) {

  private val shim = version match {
    case "0.12" => new Shim_v0_12()
    case "0.13" => new Shim_v0_13()
    case "0.14" => new Shim_v0_14()
    case "1.0" => new Shim_v1_0()
    case "1.1" => new Shim_v1_1()
    case "1.2" => new Shim_v1_2()
    case "2.0" => new Shim_v2_0()
    case "2.1" => new Shim_v2_1()
    case "2.2" => new Shim_v2_2()
    case "2.3" => new Shim_v2_3()
    case "3.0" => new Shim_v3_0()
    case "3.1" => new Shim_v3_1()
  }

  private def hive: Hive = {
    val hiveConf = new HiveConf(classOf[SessionState])
    lazy val warehousePath = Utils.createTempDir()

    hiveConf.set("datanucleus.schema.autoCreateAll", "true")
    hiveConf.set("hive.metastore.schema.verification", "false")
    hiveConf.set("hive.metastore.warehouse.dir", warehousePath.toString)

    getHive(hiveConf)
  }

  test("createTables") {
    shim.createTable(hive, new Table("default", "table1"), ifNotExists = true)
  }

  test("getTablesByType") {
    if (version >= "2.3") {
      val managed = shim.getTablesByType(hive, "default", "table1", TableType.MANAGED_TABLE)
      val external = shim.getTablesByType(hive, "default", "table1", TableType.EXTERNAL_TABLE)

      assert(managed === Seq("table1"))
      assert(external === Seq.empty)
    } else {
      val e = intercept[SparkUnsupportedOperationException] {
        shim.getTablesByType(hive, "default", "table1", TableType.MANAGED_TABLE)
      }
      checkError(e, errorClass = "GET_TABLES_BY_TYPE_UNSUPPORTED_BY_HIVE_VERSION")
    }
  }

  test("dropTable") {
    shim.dropTable(hive, "default", "table1")
  }
}
