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

package org.apache.spark.sql.sources.v2.state

import org.scalatest.{Assertions, BeforeAndAfterAll}

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.datasources.v2.state.{SchemaUtil, StateSchemaExtractor}
import org.apache.spark.sql.execution.datasources.v2.state.StateSchemaExtractor.StateKind
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class StateSchemaExtractorSuite
  extends StateStoreTestBase
  with BeforeAndAfterAll
  with Assertions {

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  Seq(1, 2).foreach { ver =>
    test(s"extract schema from streaming aggregation query - state format v$ver") {
      withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> ver.toString) {
        val aggregated = getCompositeKeyStreamingAggregationQuery

        val stateSchema = getSchemaForCompositeKeyStreamingAggregationQuery(ver)
        val expectedKeySchema = SchemaUtil.getSchemaAsDataType(stateSchema, "key")
          .asInstanceOf[StructType]
        val expectedValueSchema = SchemaUtil.getSchemaAsDataType(stateSchema, "value")
          .asInstanceOf[StructType]

        val schemaInfos = new StateSchemaExtractor(spark).extract(aggregated.toDF())
        assert(schemaInfos.length === 1)
        val schemaInfo = schemaInfos.head
        assert(schemaInfo.opId === 0)
        assert(schemaInfo.formatVersion === ver)
        assert(schemaInfo.stateKind === StateKind.StreamingAggregation)

        assert(compareSchemaWithoutName(schemaInfo.keySchema, expectedKeySchema),
          s"Even without column names, ${schemaInfo.keySchema} did not equal $expectedKeySchema")
        assert(compareSchemaWithoutName(schemaInfo.valueSchema, expectedValueSchema),
          s"Even without column names, ${schemaInfo.valueSchema} did not equal " +
            s"$expectedValueSchema")
      }
    }
  }

  Seq(1, 2).foreach { ver =>
    test(s"extract schema from flatMapGroupsWithState query - state format v$ver") {
      withSQLConf(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> ver.toString) {
        // This is borrowed from StateStoreTest, runFlatMapGroupsWithStateQuery
        val aggregated = getFlatMapGroupsWithStateQuery

        val expectedKeySchema = new StructType().add("value", StringType, nullable = true)

        val expectedValueSchema = if (ver == 1) {
          Encoders.product[SessionInfo].schema
            .add("timeoutTimestamp", IntegerType, nullable = false)
        } else {
          // ver == 2
          new StructType()
            .add("groupState", Encoders.product[SessionInfo].schema)
            .add("timeoutTimestamp", LongType, nullable = false)
        }

        val schemaInfos = new StateSchemaExtractor(spark).extract(aggregated.toDF())
        assert(schemaInfos.length === 1)
        val schemaInfo = schemaInfos.head
        assert(schemaInfo.opId === 0)
        assert(schemaInfo.stateKind === StateKind.FlatMapGroupsWithState)
        assert(schemaInfo.formatVersion === ver)

        assert(compareSchemaWithoutName(schemaInfo.keySchema, expectedKeySchema),
          s"Even without column names, ${schemaInfo.keySchema} did not equal $expectedKeySchema")
        assert(compareSchemaWithoutName(schemaInfo.valueSchema, expectedValueSchema),
          s"Even without column names, ${schemaInfo.valueSchema} did not equal " +
            s"$expectedValueSchema")
      }
    }
  }

  private def compareSchemaWithoutName(s1: StructType, s2: StructType): Boolean = {
    if (s1.length != s2.length) {
      false
    } else {
      s1.zip(s2).forall { case (column1, column2) =>
        column1.dataType == column2.dataType && column1.nullable == column2.nullable
      }
    }
  }
}
