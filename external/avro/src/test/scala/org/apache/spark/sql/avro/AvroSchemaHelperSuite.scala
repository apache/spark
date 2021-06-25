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
package org.apache.spark.sql.avro

import org.apache.avro.SchemaBuilder

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class AvroSchemaHelperSuite extends SQLTestUtils with SharedSparkSession {

  test("ensure schema is a record") {
    val avroSchema = SchemaBuilder.builder().intType()

    val msg = intercept[IncompatibleSchemaException] {
      new AvroUtils.AvroSchemaHelper(avroSchema)
    }.getMessage
    assert(msg.contains("Attempting to treat int as a RECORD"))
  }

  test("handle mixed case field names") {
    val catalystSchema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", IntegerType) ::
      StructField("A", IntegerType) ::
      Nil
    )

    val avroSchema = SchemaConverters.toAvroType(catalystSchema)
    val helper = new AvroUtils.AvroSchemaHelper(avroSchema)
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      assert(helper.getFieldByName("A").get.name() == "A")
      assert(helper.getFieldByName("a").get.name() == "a")
      assert(helper.getFieldByName("b").get.name() == "b")
      assert(helper.getFieldByName("B").isEmpty)
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      Seq("a", "A").foreach { fieldName =>
        withClue(s"looking for field name: $fieldName") {
          val msg = intercept[IncompatibleSchemaException] {
            helper.getFieldByName(fieldName)
          }.getMessage
          assert(msg.contains(s"Searching for '$fieldName' in Avro schema"))
        }
      }

      assert(helper.getFieldByName("b").get.name() == "b")
      assert(helper.getFieldByName("B").get.name() == "b")
    }
  }
}
