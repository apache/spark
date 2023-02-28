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

package org.apache.spark.sql
import org.apache.spark.sql.connect.client.util.{IntegrationTestUtils, RemoteSparkSession}
import org.apache.spark.sql.types._

class DataFrameNaFunctionSuite extends RemoteSparkSession {

  def createDF(): DataFrame = {
    val testDataPath = java.nio.file.Paths
      .get(
        IntegrationTestUtils.sparkHome,
        "connector",
        "connect",
        "common",
        "src",
        "test",
        "resources",
        "query-tests",
        "test-data",
        "people.gz.parquet")
      .toAbsolutePath
    spark.read
      .format("parquet")
      .option("path", testDataPath.toString)
      .schema(
        StructType(
          StructField("name", StringType) ::
            StructField("age", IntegerType) ::
            StructField("height", DoubleType) :: Nil))
      .load()
  }

  test("drop") {
    val input = createDF()
    input.show()
    val rows = input.collect()

    val dropResult1 = input.na.drop("name" :: Nil).select("name").collect()
    val dropExpected1 = Array(Row("Bob"), Row("Alice"), Row("David"), Row("Nina"), Row("Amy"))
    assert(dropResult1 === dropExpected1)

    val dropResult2 = input.na.drop("age" :: Nil).select("name").collect()
    val dropExpected2 = Array(Row("Bob"), Row("David"), Row("Nina"))
    assert(dropResult2 === dropExpected2)

    val dropResult3 = input.na.drop("age" :: "height" :: Nil).collect()
    val dropExpected3 = Array(Row(0))
    assert(dropResult3 === dropExpected3)
  }

  test("fill") {

  }

  test("replace") {

  }
}
