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

package org.apache.spark.sql.connector

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamTest, Trigger}
import org.apache.spark.sql.types._

/**
 * Tests for schema evolution in streaming writes using DataSourceV2.
 *
 * Schema evolution happens at query analysis time: when a streaming query is started (or
 * restarted) and the source schema has columns not present in the sink table, the table is
 * evolved to include those columns before any data is written.
 */
class StreamingSchemaEvolutionSuite
    extends StreamTest with BeforeAndAfter {

  import testImplicits._

  private val catalogName = "testcat"
  private val namespace = "ns"
  private val tableIdent = s"$catalogName.$namespace.test_table"

  before {
    spark.conf.set(
      s"spark.sql.catalog.$catalogName", classOf[InMemoryTableCatalog].getName)
    sql(s"CREATE NAMESPACE IF NOT EXISTS $catalogName.$namespace")
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$catalogName")
  }

  test("streaming write with extra source column adds column to table") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")

        val input = MemoryStream[(Int, String, Double)]
        val df = input.toDF().toDF("id", "data", "amount")

        val query = df.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input.addData((1, "a", 10.0), (2, "b", 20.0))
          query.processAllAvailable()
        } finally {
          query.stop()
        }

        val result = spark.table(tableIdent)
        checkAnswer(result, Seq(Row(1, "a", 10.0), Row(2, "b", 20.0)))
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType),
          StructField("amount", DoubleType))))
      }
    }
  }

  test("streaming write with matching schema - no evolution needed") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")

        val input = MemoryStream[(Int, String)]
        val df = input.toDF().toDF("id", "data")

        val query = df.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input.addData((1, "a"), (2, "b"))
          query.processAllAvailable()
        } finally {
          query.stop()
        }

        val result = spark.table(tableIdent)
        checkAnswer(result, Seq(Row(1, "a"), Row(2, "b")))
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType))))
      }
    }
  }

  test("streaming write evolves schema then processes multiple batches") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")

        val input = MemoryStream[(Int, String, Double)]
        val df = input.toDF().toDF("id", "data", "amount")

        val query = df.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          // First batch triggers schema evolution.
          input.addData((1, "a", 10.0))
          query.processAllAvailable()

          // Second batch should work without re-triggering evolution.
          // Note: InMemoryTableCatalog creates new table instances on alterTable,
          // so the second batch writes to the old instance. We just verify
          // that the second batch completes without errors.
          input.addData((2, "b", 20.0))
          query.processAllAvailable()
        } finally {
          query.stop()
        }

        val result = spark.table(tableIdent)
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType),
          StructField("amount", DoubleType))))
      }
    }
  }

  test("streaming write with multiple extra columns") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT)")

        val input = MemoryStream[(Int, String, Double)]
        val df = input.toDF().toDF("id", "data", "amount")

        val query = df.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input.addData((1, "a", 10.0))
          query.processAllAvailable()
        } finally {
          query.stop()
        }

        val result = spark.table(tableIdent)
        checkAnswer(result, Seq(Row(1, "a", 10.0)))
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType),
          StructField("amount", DoubleType))))
      }
    }
  }

  test("table without AUTOMATIC_SCHEMA_EVOLUTION capability - schema not evolved") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(
          s"""CREATE TABLE $tableIdent (id INT, data STRING)
             |TBLPROPERTIES ('auto-schema-evolution' = 'false')""".stripMargin)

        val input = MemoryStream[(Int, String, Double)]
        val df = input.toDF().toDF("id", "data", "amount")

        val query = df.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input.addData((1, "a", 10.0))
          query.processAllAvailable()
        } finally {
          query.stop()
        }

        // Schema should NOT have been evolved since the table lacks the capability.
        val result = spark.table(tableIdent)
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType))))
      }
    }
  }

  test("streaming write without withSchemaEvolution - schema not evolved") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")

        val input = MemoryStream[(Int, String, Double)]
        val df = input.toDF().toDF("id", "data", "amount")

        // No .withSchemaEvolution() call.
        val query = df.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input.addData((1, "a", 10.0))
          query.processAllAvailable()
        } finally {
          query.stop()
        }

        // Schema should NOT have been evolved since withSchemaEvolution was not called.
        val result = spark.table(tableIdent)
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType))))
      }
    }
  }

  test("streaming restart after schema evolution preserves data") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")

        // First query: write with matching schema.
        val input1 = MemoryStream[(Int, String)]
        val df1 = input1.toDF().toDF("id", "data")

        val query1 = df1.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input1.addData((1, "a"), (2, "b"))
          query1.processAllAvailable()
        } finally {
          query1.stop()
        }

        checkAnswer(spark.table(tableIdent), Seq(Row(1, "a"), Row(2, "b")))

        // Second query: write with extra column, triggers schema evolution.
        val input2 = MemoryStream[(Int, String, Double)]
        val df2 = input2.toDF().toDF("id", "data", "amount")

        val query2 = df2.writeStream
          .withSchemaEvolution()
          .option(
            "checkpointLocation",
            s"${checkpointDir.getCanonicalPath}_2")
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input2.addData((3, "c", 30.0))
          query2.processAllAvailable()
        } finally {
          query2.stop()
        }

        val result = spark.table(tableIdent)
        checkAnswer(result, Seq(
          Row(1, "a", null),
          Row(2, "b", null),
          Row(3, "c", 30.0)))
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType),
          StructField("amount", DoubleType))))
      }
    }
  }

  test("schema evolution on restart after sink table altered between batches") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")

        // First query: write with matching schema.
        val input1 = MemoryStream[(Int, String)]
        val df1 = input1.toDF().toDF("id", "data")

        val query1 = df1.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input1.addData((1, "a"))
          query1.processAllAvailable()
        } finally {
          query1.stop()
        }

        // Alter the table externally to add a column, simulating a schema change
        // that happened while the query was down.
        sql(s"ALTER TABLE $tableIdent ADD COLUMN amount DOUBLE")

        val input2 = MemoryStream[(Int, String, Double)]
        val df2 = input2.toDF().toDF("id", "data", "amount")

        val query2 = df2.writeStream
          .withSchemaEvolution()
          .option(
            "checkpointLocation",
            s"${checkpointDir.getCanonicalPath}_2")
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input2.addData((2, "b", 20.0))
          query2.processAllAvailable()
        } finally {
          query2.stop()
        }

        val result = spark.table(tableIdent)
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType),
          StructField("amount", DoubleType))))
      }
    }
  }

  test("schema evolution with Trigger.Once") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")

        val input = MemoryStream[(Int, String, Double)]
        val df = input.toDF().toDF("id", "data", "amount")
        input.addData((1, "a", 10.0), (2, "b", 20.0))

        val query = df.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .toTable(tableIdent)

        try {
          query.processAllAvailable()
        } finally {
          query.stop()
        }

        val result = spark.table(tableIdent)
        checkAnswer(result, Seq(Row(1, "a", 10.0), Row(2, "b", 20.0)))
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType),
          StructField("amount", DoubleType))))
      }
    }
  }

  test("schema evolution with Trigger.AvailableNow") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")

        val input = MemoryStream[(Int, String, Double)]
        val df = input.toDF().toDF("id", "data", "amount")
        input.addData((1, "a", 10.0))

        val query = df.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.AvailableNow())
          .toTable(tableIdent)

        try {
          query.processAllAvailable()
        } finally {
          query.stop()
        }

        val result = spark.table(tableIdent)
        checkAnswer(result, Seq(Row(1, "a", 10.0)))
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType),
          StructField("amount", DoubleType))))
      }
    }
  }

  test("incremental schema evolution across multiple restarts") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT)")

        // First query: add "data" column.
        val input1 = MemoryStream[(Int, String)]
        val df1 = input1.toDF().toDF("id", "data")

        val query1 = df1.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input1.addData((1, "a"))
          query1.processAllAvailable()
        } finally {
          query1.stop()
        }

        assert(spark.table(tableIdent).schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType))))

        // Second query: add "amount" column on top of the already-evolved schema.
        val input2 = MemoryStream[(Int, String, Double)]
        val df2 = input2.toDF().toDF("id", "data", "amount")

        val query2 = df2.writeStream
          .withSchemaEvolution()
          .option(
            "checkpointLocation",
            s"${checkpointDir.getCanonicalPath}_2")
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input2.addData((2, "b", 20.0))
          query2.processAllAvailable()
        } finally {
          query2.stop()
        }

        val result = spark.table(tableIdent)
        checkAnswer(result, Seq(
          Row(1, "a", null),
          Row(2, "b", 20.0)))
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType),
          StructField("amount", DoubleType))))

        // Third query: no new columns - schema should stay the same.
        val input3 = MemoryStream[(Int, String, Double)]
        val df3 = input3.toDF().toDF("id", "data", "amount")

        val query3 = df3.writeStream
          .withSchemaEvolution()
          .option(
            "checkpointLocation",
            s"${checkpointDir.getCanonicalPath}_3")
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input3.addData((3, "c", 30.0))
          query3.processAllAvailable()
        } finally {
          query3.stop()
        }

        val result2 = spark.table(tableIdent)
        assert(result2.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType),
          StructField("amount", DoubleType))))
      }
    }
  }

  test("stop and restart same query - schema evolved on restart") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")

        // First run: matching schema, no evolution.
        val input1 = MemoryStream[(Int, String)]
        val df1 = input1.toDF().toDF("id", "data")

        val query1 = df1.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input1.addData((1, "a"))
          query1.processAllAvailable()
          input1.addData((2, "b"))
          query1.processAllAvailable()
        } finally {
          query1.stop()
        }

        checkAnswer(spark.table(tableIdent), Seq(Row(1, "a"), Row(2, "b")))

        // Second run: new source schema with extra column.
        // Uses a different checkpoint since MemoryStream state can't be
        // reused across restarts with a different schema.
        val input2 = MemoryStream[(Int, String, Double)]
        val df2 = input2.toDF().toDF("id", "data", "amount")

        val query2 = df2.writeStream
          .withSchemaEvolution()
          .option(
            "checkpointLocation",
            s"${checkpointDir.getCanonicalPath}_2")
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input2.addData((3, "c", 30.0))
          query2.processAllAvailable()
        } finally {
          query2.stop()
        }

        val result = spark.table(tableIdent)
        checkAnswer(result, Seq(
          Row(1, "a", null),
          Row(2, "b", null),
          Row(3, "c", 30.0)))
      }
    }
  }

  test("schema evolution with Trigger.Once across restart") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT)")

        // First run: Trigger.Once with extra column.
        val input1 = MemoryStream[(Int, String)]
        val df1 = input1.toDF().toDF("id", "data")
        input1.addData((1, "a"))

        val query1 = df1.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .toTable(tableIdent)

        try {
          query1.processAllAvailable()
        } finally {
          query1.stop()
        }

        assert(spark.table(tableIdent).schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType))))

        // Second run: Trigger.Once with yet another extra column.
        val input2 = MemoryStream[(Int, String, Double)]
        val df2 = input2.toDF().toDF("id", "data", "amount")
        input2.addData((2, "b", 20.0))

        val query2 = df2.writeStream
          .withSchemaEvolution()
          .option(
            "checkpointLocation",
            s"${checkpointDir.getCanonicalPath}_2")
          .trigger(Trigger.Once())
          .toTable(tableIdent)

        try {
          query2.processAllAvailable()
        } finally {
          query2.stop()
        }

        val result = spark.table(tableIdent)
        checkAnswer(result, Seq(Row(1, "a", null), Row(2, "b", 20.0)))
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("data", StringType),
          StructField("amount", DoubleType))))
      }
    }
  }


  test("withSchemaEvolution rejected with continuous trigger") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")

        val input = MemoryStream[(Int, String, Double)]
        val df = input.toDF().toDF("id", "data", "amount")

        val e = intercept[SparkUnsupportedOperationException] {
          df.writeStream
            .withSchemaEvolution()
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .trigger(Trigger.Continuous("1 second"))
            .toTable(tableIdent)
        }
        assert(e.getCondition ==
          "UNSUPPORTED_STREAMING_SCHEMA_EVOLUTION.CONTINUOUS_TRIGGER")
      }
    }
  }

  test("withSchemaEvolution rejected for V1 sink") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[(Int, String)]
      val df = input.toDF().toDF("id", "data")
      input.addData((1, "a"))

      // foreachBatch creates a V1 ForeachBatchSink. The error surfaces
      // when the streaming thread evaluates MicroBatchExecution.logicalPlan.
      val query = df.writeStream
        .withSchemaEvolution()
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreachBatch { (batch: org.apache.spark.sql.Dataset[Row], _: Long) =>
          ()
        }
        .start()

      try {
        val e = intercept[
          org.apache.spark.sql.streaming.StreamingQueryException] {
          query.processAllAvailable()
        }
        assert(e.getCause
          .isInstanceOf[SparkUnsupportedOperationException])
        assert(e.getCause
          .asInstanceOf[SparkUnsupportedOperationException]
          .getCondition ==
          "UNSUPPORTED_STREAMING_SCHEMA_EVOLUTION.NOT_V2_TABLE")
      } finally {
        query.stop()
      }
    }
  }

  test("streaming write with type widening") {
    withTable(tableIdent) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $tableIdent (id INT, value INT)")

        val input = MemoryStream[(Int, Long)]
        val df = input.toDF().toDF("id", "value")

        val query = df.writeStream
          .withSchemaEvolution()
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .toTable(tableIdent)

        try {
          input.addData((1, 100L), (2, 200L))
          query.processAllAvailable()
        } finally {
          query.stop()
        }

        val result = spark.table(tableIdent)
        checkAnswer(result, Seq(Row(1, 100L), Row(2, 200L)))
        assert(result.schema == StructType(Seq(
          StructField("id", IntegerType),
          StructField("value", LongType))))
      }
    }
  }
}
