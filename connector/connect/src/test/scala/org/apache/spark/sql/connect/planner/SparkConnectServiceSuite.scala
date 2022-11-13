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
package org.apache.spark.sql.connect.planner

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Testing Connect Service implementation.
 */
class SparkConnectServiceSuite extends SharedSparkSession {

  test("Test schema in analyze response") {
    withTable("test") {
      spark.sql("""
          | CREATE TABLE test (col1 INT, col2 STRING)
          | USING parquet
          |""".stripMargin)

      val instance = new SparkConnectService(false)
      val relation = proto.Relation
        .newBuilder()
        .setRead(
          proto.Read
            .newBuilder()
            .setNamedTable(proto.Read.NamedTable.newBuilder.setUnparsedIdentifier("test").build())
            .build())
        .build()

      val response = instance.handleAnalyzePlanRequest(relation, spark)

      assert(response.getSchema.hasStruct)
      val schema = response.getSchema.getStruct
      assert(schema.getFieldsCount == 2)
      assert(
        schema.getFields(0).getName == "col1"
          && schema.getFields(0).getType.getKindCase == proto.DataType.KindCase.I32)
      assert(
        schema.getFields(1).getName == "col2"
          && schema.getFields(1).getType.getKindCase == proto.DataType.KindCase.STRING)
    }
  }
}
