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

package org.apache.spark.sql.streaming

import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.streaming.Unassigned
import org.apache.spark.sql.execution.streaming.runtime.StreamingRelation
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for streaming source identifying name propagation through the resolution pipeline.
 * These tests verify that sourceIdentifyingName is correctly propagated from CatalogTable
 * through DataSource to StreamingRelation during streaming table resolution.
 */
class StreamingSourceIdentifyingNameSuite extends SharedSparkSession {

  test("STREAM table resolution propagates sourceIdentifyingName through pipeline") {
    withTable("stream_name_test") {
      sql("CREATE TABLE stream_name_test (id INT) USING PARQUET")

      val analyzedPlan = sql("SELECT * FROM STREAM stream_name_test").queryExecution.analyzed

      val streamingRelation = analyzedPlan.collectFirst {
        case sr: StreamingRelation => sr
      }

      assert(streamingRelation.isDefined, "Expected StreamingRelation in analyzed plan")
      assert(streamingRelation.get.sourceIdentifyingName == Unassigned,
        s"Expected Unassigned but got ${streamingRelation.get.sourceIdentifyingName}")

      // Verify the DataSource has the sourceIdentifyingName set
      val dsSourceName = streamingRelation.get.dataSource.userSpecifiedStreamingSourceName
      assert(dsSourceName == Some(Unassigned),
        s"Expected Some(Unassigned) but got $dsSourceName")
    }
  }

  test("STREAM with qualified name propagates sourceIdentifyingName through SubqueryAlias") {
    // When querying a table with a qualified name (e.g., spark_catalog.default.t),
    // the catalog lookup creates a SubqueryAlias wrapper.
    withTable("stream_alias_test") {
      sql("CREATE TABLE stream_alias_test (id INT, data STRING) USING PARQUET")

      // Query using fully qualified name to ensure SubqueryAlias is created
      val analyzedPlan = sql(
        "SELECT * FROM STREAM spark_catalog.default.stream_alias_test"
      ).queryExecution.analyzed

      val streamingRelation = analyzedPlan.collectFirst {
        case sr: StreamingRelation => sr
      }

      assert(streamingRelation.isDefined, "Expected StreamingRelation in analyzed plan")
      assert(streamingRelation.get.sourceIdentifyingName == Unassigned,
        s"Expected Unassigned but got ${streamingRelation.get.sourceIdentifyingName}")

      // Verify the plan has SubqueryAlias wrapping StreamingRelation
      val hasSubqueryAlias = analyzedPlan.collect {
        case SubqueryAlias(_, _: StreamingRelation) => true
      }.nonEmpty
      assert(hasSubqueryAlias, "Expected SubqueryAlias wrapping StreamingRelation")
    }
  }

  test("readStream.table() API resolves streaming table correctly") {
    // This tests the fallback case in FindDataSourceTable for streaming
    // UnresolvedCatalogRelation that is NOT wrapped in NamedStreamingRelation.
    withTable("api_stream_test") {
      sql("CREATE TABLE api_stream_test (id INT) USING PARQUET")

      val df = spark.readStream.table("api_stream_test")
      val analyzedPlan = df.queryExecution.analyzed

      val streamingRelation = analyzedPlan.collectFirst {
        case sr: StreamingRelation => sr
      }

      assert(streamingRelation.isDefined, "Expected StreamingRelation in analyzed plan")
      assert(streamingRelation.get.sourceIdentifyingName == Unassigned,
        s"Expected Unassigned but got ${streamingRelation.get.sourceIdentifyingName}")
    }
  }

  test("batch table query still resolves correctly (regression test)") {
    // Verify that the !isStreaming guard in FindDataSourceTable
    // doesn't break normal batch table resolution.
    withTable("batch_test") {
      sql("CREATE TABLE batch_test (id INT, value STRING) USING PARQUET")
      sql("INSERT INTO batch_test VALUES (1, 'a'), (2, 'b')")

      val result = sql("SELECT * FROM batch_test").collect()
      assert(result.length == 2, s"Expected 2 rows but got ${result.length}")

      val analyzedPlan = sql("SELECT * FROM batch_test").queryExecution.analyzed
      val hasStreamingRelation = analyzedPlan.collect {
        case _: StreamingRelation => true
      }.nonEmpty
      assert(!hasStreamingRelation, "Batch query should not contain StreamingRelation")
    }
  }
}
