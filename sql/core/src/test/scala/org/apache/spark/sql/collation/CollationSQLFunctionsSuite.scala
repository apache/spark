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

package org.apache.spark.sql.collation

import org.apache.spark.sql.{Column, Dataset, QueryTest}
import org.apache.spark.sql.functions.{from_json, from_xml}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class CollationSQLFunctionsSuite extends QueryTest with SharedSparkSession {

  test("SPARK-50214: from_json and from_xml work correctly with session collation") {
    import testImplicits._

    def checkSchema(
        dataset: Dataset[String],
        transformation: Column,
        expectedSchema: StructType): Unit = {
      val transformedSchema = dataset.select(transformation.as("result")).schema
      assert(transformedSchema === expectedSchema)
    }

    Seq(
      StringType,
      StringType("UTF8_BINARY"),
      StringType("UNICODE"),
      StringType("UNICODE_CI_AI")).foreach { stringType =>
      val dataSchema = StructType(Seq(StructField("fieldName", stringType)))
      val expectedSchema = StructType(Seq(StructField("result", dataSchema)))

      // JSON Test
      val jsonData = Seq("""{"fieldName": "fieldValue"}""")
      val jsonDataset = spark.createDataset(jsonData)
      checkSchema(jsonDataset, from_json($"value", dataSchema), expectedSchema)

      // XML Test
      val xmlData = Seq("<root><fieldName>fieldValue</fieldName></root>")
      val xmlDataset = spark.createDataset(xmlData)
      checkSchema(xmlDataset, from_xml($"value", dataSchema), expectedSchema)
    }
  }
}
