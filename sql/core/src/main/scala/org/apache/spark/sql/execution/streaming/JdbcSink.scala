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

package org.apache.spark.sql.execution.streaming

import java.sql.Connection

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._






class JdbcSink(sqlContext: SQLContext,
               parameters: Map[String, String],
               partitionColumns: Seq[String],
               outputMode: OutputMode) extends Sink  with Logging {
  val options = new JDBCOptions(parameters)
  val sinkLog = new JDBCSinkLog(parameters, sqlContext.sparkSession)
  // If user specifies a batchIdCol in the parameters, then it means that the user wants exactly
  // once semantics. This column will store the batch Id for the row when an uncommitted batch
  // is replayed, JDBC SInk will delete the rows that were added to the previous play of the
  // batch
  val batchIdCol = parameters.get("batchIdCol")
  def addBatch(batchId: Long, df: DataFrame): Unit = {

    val schema: StructType = batchIdCol.map(colName => df.schema.add(colName, LongType, false))
                                        .getOrElse(df.schema)
    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      if (sinkLog.isBatchCommitted(batchId, conn)) {
        logInfo(s"Skipping already committed batch $batchId")
      } else {
        sinkLog.startBatch(batchId, conn)
        val isCaseSensitive = sqlContext.conf.caseSensitiveAnalysis

        val tableExists = JdbcUtils.tableExists(conn, options)
        if (tableExists) {
          if (outputMode == OutputMode.Complete()) {

            if (options.isTruncate && isCascadingTruncateTable(options.url).contains(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, options.table)
              saveRows(df, isCaseSensitive, options, batchId)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, options.table)
              createTable(conn, df, options)
              saveRows(df, isCaseSensitive, options, batchId)
            }
          } else if (outputMode == OutputMode.Append()) {
            saveRows(df, isCaseSensitive, options, batchId)
          } else {
            throw new IllegalArgumentException(s"$outputMode not supported")
          }
        } else {
          createTable(conn, df, options)
          saveRows(df, isCaseSensitive, options, batchId)
        }
        sinkLog.commitBatch(batchId, conn)

      }
    } finally {
      conn.close()
    }
  }

  /**
   * Saves the RDD to the database in a single transaction.
   */
  def saveRows(
                 df: DataFrame,
                 isCaseSensitive: Boolean,
                 options: JDBCOptions,
                 batchId: Long): Unit = {
    val url = options.url
    val table = options.table
    val dialect = JdbcDialects.get(url)
    val getConnection: () => Connection = createConnectionFactory(options)
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw new IllegalArgumentException(
        s"Invalid value `$n` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " +
          "via JDBC. The minimum value is 1.")
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }
    if(batchIdCol.isEmpty) {

      val insertStmt = getInsertStatement(table, df.schema, None, isCaseSensitive, dialect)
      val rddSchema = df.schema
      repartitionedDF.queryExecution.toRdd.foreachPartition(iterator => {
        saveInternalPartition(
          getConnection, table, iterator, rddSchema, insertStmt, batchSize, dialect, isolationLevel)
      })
    } else {

      // batchId col is defined.. construct a schema by adding the batchId col to the DF schema
      // also put the value of the batch id to the end of every row in the DF
      val dfSchema = df.schema
      val rddSchema: StructType = df.schema.add(batchIdCol.get, LongType, false)
      val insertStmt = getInsertStatement(table, rddSchema, None, isCaseSensitive, dialect)
      repartitionedDF.queryExecution.toRdd.foreachPartition(iterator => {
        saveInternalPartition(
          getConnection, table
          , iterator.map(ir => InternalRow.fromSeq(ir.toSeq(dfSchema) :+ batchId ))
          , rddSchema, insertStmt, batchSize, dialect, isolationLevel)
      })
    }
  }





  def saveMode(outputMode: OutputMode): SaveMode = {
    if (outputMode==OutputMode.Append()) {
      SaveMode.Append
    } else if (outputMode==OutputMode.Complete()) {
      SaveMode.Overwrite
    } else {
      throw new IllegalArgumentException(s"Output mode $outputMode is not supported by JdbcSink")
    }
  }
}
