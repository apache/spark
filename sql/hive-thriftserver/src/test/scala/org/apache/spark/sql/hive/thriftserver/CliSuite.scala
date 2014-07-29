/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver

import java.io.{BufferedReader, InputStreamReader, PrintWriter}

import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CliSuite extends FunSuite with BeforeAndAfterAll with TestUtils {
  val WAREHOUSE_PATH = TestUtils.getWarehousePath("cli")
  val METASTORE_PATH = TestUtils.getMetastorePath("cli")

  override def beforeAll() {
    val pb = new ProcessBuilder(
      "../../bin/spark-sql",
      "--master",
      "local",
      "--hiveconf",
      s"javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$METASTORE_PATH;create=true",
      "--hiveconf",
      "hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)

    process = pb.start()
    outputWriter = new PrintWriter(process.getOutputStream, true)
    inputReader = new BufferedReader(new InputStreamReader(process.getInputStream))
    errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream))
    waitForOutput(inputReader, "spark-sql>")
  }

  override def afterAll() {
    process.destroy()
    process.waitFor()
  }

  test("simple commands") {
    val dataFilePath = getDataFile("data/files/small_kv.txt")
    executeQuery("create table hive_test1(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table hive_test1;")
    executeQuery("cache table hive_test1", "Time taken")
  }
}
