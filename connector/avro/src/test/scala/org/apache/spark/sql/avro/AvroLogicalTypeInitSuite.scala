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

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DecimalType

/**
 * Test suite for Avro logical type initialization.
 * Tests here must run in isolation, otherwise some other test might
 * initialize variable and make this test flaky
 */
abstract class AvroLogicalTypeInitSuite
  extends QueryTest
  with SharedSparkSession {

  test("SPARK-47739: custom logical type registration test") {
    val avroTypeJson =
      """
         |{
         |  "type": "record",
         |  "name": "Entry",
         |  "fields": [
         |    {
         |      "name": "test_col",
         |      "type": [
         |        "null",
         |        {
         |          "type": "long",
         |          "logicalType": "custom-decimal",
         |          "precision": 38,
         |          "scale": 9
         |        }
         |      ],
         |      "default": null
         |    }
         |  ]
         |}
         |
   """.stripMargin

    val df = spark.read.format("avro").option("avroSchema", avroTypeJson).load()
    assert(df.schema.fields(0).dataType == DecimalType(38, 9))
  }
}

class AvroV1LogicalTypeInitSuite extends AvroLogicalTypeInitSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "avro")
}

class AvroV2LogicalTypeInitSuite extends AvroLogicalTypeInitSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
