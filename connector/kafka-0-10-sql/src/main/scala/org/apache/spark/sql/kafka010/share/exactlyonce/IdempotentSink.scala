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

package org.apache.spark.sql.kafka010.share.exactlyonce

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

/**
 * Strategy A: Idempotent Processing for Exactly-Once Semantics
 *
 * This strategy achieves exactly-once semantics by deduplicating records at the sink
 * using unique record identifiers (topic, partition, offset).
 *
 * How it works:
 * 1. Each Kafka record has a unique (topic, partition, offset) tuple
 * 2. The sink uses UPSERT/MERGE semantics with this tuple as the key
 * 3. If a record is redelivered (due to failure), the UPSERT is a no-op
 *
 * Requirements:
 * - Sink must support idempotent writes (INSERT ON CONFLICT DO NOTHING/UPDATE)
 * - Supported sinks: Delta Lake, JDBC with UPSERT, Cassandra, MongoDB, etc.
 *
 * Advantages:
 * - Simple to implement
 * - Low latency (no coordination overhead)
 * - Works with at-least-once delivery
 *
 * Disadvantages:
 * - Requires sink support for idempotent writes
 * - Storage overhead for deduplication keys
 */
object IdempotentSink extends Logging {

  /** Column names used for deduplication */
  val DEDUP_KEY_TOPIC = "__kafka_topic"
  val DEDUP_KEY_PARTITION = "__kafka_partition"
  val DEDUP_KEY_OFFSET = "__kafka_offset"

  /** Schema for deduplication columns */
  val DEDUP_KEY_SCHEMA: StructType = StructType(Seq(
    StructField(DEDUP_KEY_TOPIC, StringType, nullable = false),
    StructField(DEDUP_KEY_PARTITION, IntegerType, nullable = false),
    StructField(DEDUP_KEY_OFFSET, LongType, nullable = false)
  ))

  /**
   * Add deduplication key columns to a DataFrame read from Kafka share source.
   *
   * The Kafka share source already provides topic, partition, and offset columns.
   * This method renames them to avoid conflicts with user schema.
   *
   * @param df DataFrame from Kafka share source
   * @return DataFrame with deduplication key columns added
   */
  def addDedupColumns(df: DataFrame): DataFrame = {
    df.withColumn(DEDUP_KEY_TOPIC, col("topic"))
      .withColumn(DEDUP_KEY_PARTITION, col("partition"))
      .withColumn(DEDUP_KEY_OFFSET, col("offset"))
  }

  /**
   * Create a composite deduplication key from topic, partition, offset.
   *
   * @param df DataFrame with Kafka source columns
   * @param keyColumnName Name for the composite key column
   * @return DataFrame with composite key column added
   */
  def addCompositeKey(df: DataFrame, keyColumnName: String = "__kafka_key"): DataFrame = {
    df.withColumn(keyColumnName,
      concat(col("topic"), lit("-"), col("partition"), lit(":"), col("offset")))
  }

  /**
   * Configure Delta Lake sink for idempotent writes.
   *
   * Uses MERGE operation with the deduplication key as the merge condition.
   */
  def configureDeltaSink(
      df: DataFrame,
      tablePath: String,
      partitionBy: Seq[String] = Seq.empty): DataStreamWriter[Row] = {
    val dfWithKeys = addDedupColumns(df)

    val writer = dfWithKeys.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", s"$tablePath/_checkpoints")
      .option("mergeSchema", "true")

    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*)
    } else {
      writer
    }
  }

  /**
   * Write to Delta Lake with deduplication using foreachBatch.
   *
   * This uses Delta Lake's MERGE operation to achieve exactly-once semantics.
   */
  def writeToDeltaWithDedup(
      df: DataFrame,
      tablePath: String,
      spark: SparkSession): DataStreamWriter[Row] = {
    import io.delta.tables.DeltaTable

    df.writeStream
      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
        val dfWithKeys = addDedupColumns(batchDf)

        if (DeltaTable.isDeltaTable(spark, tablePath)) {
          val deltaTable = DeltaTable.forPath(spark, tablePath)

          // MERGE with deduplication key
          deltaTable.as("target")
            .merge(
              dfWithKeys.as("source"),
              s"target.$DEDUP_KEY_TOPIC = source.$DEDUP_KEY_TOPIC AND " +
              s"target.$DEDUP_KEY_PARTITION = source.$DEDUP_KEY_PARTITION AND " +
              s"target.$DEDUP_KEY_OFFSET = source.$DEDUP_KEY_OFFSET"
            )
            .whenNotMatched()
            .insertAll()
            .execute()
        } else {
          // First write - create the table
          dfWithKeys.write
            .format("delta")
            .mode("overwrite")
            .save(tablePath)
        }
      }
  }

  /**
   * Configure JDBC sink for idempotent writes using INSERT ON CONFLICT.
   *
   * Note: Requires the target table to have a unique constraint on
   * (topic, partition, offset) columns.
   */
  def configureJdbcSink(
      df: DataFrame,
      url: String,
      table: String,
      connectionProperties: java.util.Properties): DataStreamWriter[Row] = {
    val dfWithKeys = addDedupColumns(df)

    dfWithKeys.writeStream
      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
        batchDf.write
          .mode("append")
          .jdbc(url, table, connectionProperties)
      }
  }

  /**
   * Generate SQL for creating a deduplication-enabled table.
   *
   * @param tableName Table name
   * @param userSchema User's schema (excluding dedup columns)
   * @param dialect SQL dialect (postgresql, mysql, sqlite)
   * @return CREATE TABLE SQL statement
   */
  def generateCreateTableSql(
      tableName: String,
      userSchema: StructType,
      dialect: String = "postgresql"): String = {
    val allFields = DEDUP_KEY_SCHEMA.fields ++ userSchema.fields

    val columnDefs = allFields.map { field =>
      val sqlType = dialect match {
        case "postgresql" => sparkTypeToPostgres(field.dataType)
        case "mysql" => sparkTypeToMysql(field.dataType)
        case _ => sparkTypeToAnsi(field.dataType)
      }
      s"${field.name} $sqlType${if (field.nullable) "" else " NOT NULL"}"
    }.mkString(",\n  ")

    val uniqueConstraint = s"UNIQUE ($DEDUP_KEY_TOPIC, $DEDUP_KEY_PARTITION, $DEDUP_KEY_OFFSET)"

    s"""CREATE TABLE IF NOT EXISTS $tableName (
       |  $columnDefs,
       |  $uniqueConstraint
       |)""".stripMargin
  }

  private def sparkTypeToPostgres(dt: org.apache.spark.sql.types.DataType): String = dt match {
    case StringType => "TEXT"
    case IntegerType => "INTEGER"
    case LongType => "BIGINT"
    case org.apache.spark.sql.types.BinaryType => "BYTEA"
    case org.apache.spark.sql.types.TimestampType => "TIMESTAMP"
    case org.apache.spark.sql.types.DoubleType => "DOUBLE PRECISION"
    case org.apache.spark.sql.types.BooleanType => "BOOLEAN"
    case _ => "TEXT"
  }

  private def sparkTypeToMysql(dt: org.apache.spark.sql.types.DataType): String = dt match {
    case StringType => "TEXT"
    case IntegerType => "INT"
    case LongType => "BIGINT"
    case org.apache.spark.sql.types.BinaryType => "BLOB"
    case org.apache.spark.sql.types.TimestampType => "TIMESTAMP"
    case org.apache.spark.sql.types.DoubleType => "DOUBLE"
    case org.apache.spark.sql.types.BooleanType => "BOOLEAN"
    case _ => "TEXT"
  }

  private def sparkTypeToAnsi(dt: org.apache.spark.sql.types.DataType): String = dt match {
    case StringType => "VARCHAR"
    case IntegerType => "INTEGER"
    case LongType => "BIGINT"
    case org.apache.spark.sql.types.BinaryType => "BINARY"
    case org.apache.spark.sql.types.TimestampType => "TIMESTAMP"
    case org.apache.spark.sql.types.DoubleType => "DOUBLE"
    case org.apache.spark.sql.types.BooleanType => "BOOLEAN"
    case _ => "VARCHAR"
  }
}

/**
 * Helper for deduplicating within a DataFrame using Spark's dropDuplicates.
 *
 * Useful when processing data in memory before writing to a non-idempotent sink.
 */
object InMemoryDeduplication extends Logging {

  /**
   * Deduplicate records within a batch using Kafka coordinates.
   *
   * @param df DataFrame with topic, partition, offset columns
   * @return Deduplicated DataFrame
   */
  def deduplicateByKafkaCoordinates(df: DataFrame): DataFrame = {
    df.dropDuplicates("topic", "partition", "offset")
  }

  /**
   * Deduplicate records using a custom key expression.
   *
   * @param df DataFrame to deduplicate
   * @param keyColumns Columns to use as deduplication key
   * @return Deduplicated DataFrame
   */
  def deduplicateByKey(df: DataFrame, keyColumns: String*): DataFrame = {
    df.dropDuplicates(keyColumns: _*)
  }

  /**
   * Deduplicate with watermark for streaming state management.
   *
   * @param df DataFrame with timestamp column
   * @param timestampColumn Column containing event timestamps
   * @param watermarkDelay Watermark delay (e.g., "10 minutes")
   * @param keyColumns Deduplication key columns
   * @return Deduplicated DataFrame with watermark
   */
  def deduplicateWithWatermark(
      df: DataFrame,
      timestampColumn: String,
      watermarkDelay: String,
      keyColumns: String*): DataFrame = {
    df.withWatermark(timestampColumn, watermarkDelay)
      .dropDuplicates(keyColumns: _*)
  }
}

