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

import java.util

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Committed, Identifier, InMemoryRowLevelOperationTable, InMemoryRowLevelOperationTableCatalog, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, PhysicalWriteInfo, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.sources.PackedRowWriterFactory
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.tags.SlowSQLTest

/**
 * Tests that structured streaming micro-batch writes participate in the DSv2 transaction API.
 *
 * The V2 streaming path is:
 *   WriteToMicroBatchDataSource (logical)
 *     -> V2Writes rule -> WriteToDataSourceV2(MicroBatchWrite) (logical)
 *     -> WriteToDataSourceV2Exec (physical, implements TransactionalExec)
 *
 * Each micro-batch runs in its own IncrementalExecution, so transactionOpt is evaluated
 * fresh per batch. The transaction is committed inside WriteToDataSourceV2Exec.run() after
 * writeWithV2 completes, and aborted if writeWithV2 throws.
 */
@SlowSQLTest
class StreamingTransactionSuite extends StreamTest with BeforeAndAfter {
  import testImplicits._

  private val tableIdent = Identifier.of(Array("ns1"), "test_table")
  private val tableNameAsString = "cat.ns1.test_table"

  before {
    spark.conf.set("spark.sql.catalog.cat",
      classOf[InMemoryRowLevelOperationTableCatalog].getName)
    sql("CREATE NAMESPACE cat.ns1")
    sql(s"CREATE TABLE $tableNameAsString (value INT) USING foo")
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
    sqlContext.streams.active.foreach(_.stop())
  }

  private def catalog: InMemoryRowLevelOperationTableCatalog =
    spark.sessionState.catalogManager.catalog("cat")
      .asInstanceOf[InMemoryRowLevelOperationTableCatalog]

  private def delegateTable: InMemoryRowLevelOperationTable =
    catalog.loadTable(tableIdent).asInstanceOf[InMemoryRowLevelOperationTable]

  test("streaming micro-batch append commits a transaction") {
    val stream = MemoryStream[Int]

    withTempDir { checkpointDir =>
      val query = stream.toDF()
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .toTable(tableNameAsString)

      try {
        stream.addData(1, 2, 3)
        query.processAllAvailable()

        val txn = catalog.lastTransaction
        assert(txn.currentState === Committed)
        assert(txn.isClosed)
        assert(delegateTable.version() === "2")

        checkAnswer(
          spark.table(tableNameAsString),
          Seq(Row(1), Row(2), Row(3)))
      } finally {
        query.stop()
      }
    }
  }

  test("each micro-batch gets a fresh transaction") {
    val stream = MemoryStream[Int]

    withTempDir { checkpointDir =>
      val query = stream.toDF()
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .toTable(tableNameAsString)

      try {
        stream.addData(1, 2, 3)
        query.processAllAvailable()
        val txn1 = catalog.lastTransaction
        assert(txn1.currentState === Committed)
        assert(txn1.isClosed)
        assert(delegateTable.version() === "2")

        stream.addData(4, 5)
        query.processAllAvailable()
        val txn2 = catalog.lastTransaction
        assert(txn2.currentState === Committed)
        assert(txn2.isClosed)
        assert(txn2 ne txn1, "each batch must open a fresh transaction")
        assert(delegateTable.version() === "3")

        checkAnswer(
          spark.table(tableNameAsString),
          Seq(Row(1), Row(2), Row(3), Row(4), Row(5)))
      } finally {
        query.stop()
      }
    }
  }

  test("no transaction is started when the catalog is not transactional") {
    // Writing to a non-transactional catalog (session catalog / parquet) must not
    // open a transaction. Verify by confirming catalog.lastTransaction is untouched.
    val initialLastTxn = catalog.lastTransaction  // null at start

    withTempDir { dir =>
      val stream = MemoryStream[Int]
      val query = stream.toDF()
        .writeStream
        .format("parquet")
        .option("checkpointLocation", dir.getCanonicalPath + "/checkpoint")
        .option("path", dir.getCanonicalPath + "/data")
        .start()

      try {
        stream.addData(1, 2, 3)
        query.processAllAvailable()

        // our TxnTableCatalog was not involved - lastTransaction must be unchanged
        assert(catalog.lastTransaction === initialLastTxn)
      } finally {
        query.stop()
      }
    }
  }

  test("no transaction is started for an anonymous V2 sink (catalog = None)") {
    // An anonymous V2 sink has DataSourceV2Relation.catalog == None (no catalog/ident).
    // UnresolveTransactionRelations skips it since catalog doesn't match any
    // TransactionalCatalogPlugin, so transactionOpt returns None and no transaction is opened.
    val initialLastTxn = catalog.lastTransaction  // null at start

    withTempDir { dir =>
      val stream = MemoryStream[Int]
      val query = stream.toDF()
        .writeStream
        .format(classOf[NoOpV2SinkProvider].getName)
        .option("checkpointLocation", dir.getCanonicalPath + "/checkpoint")
        .start()

      try {
        stream.addData(1, 2, 3)
        query.processAllAvailable()

        // Anonymous V2 sink: no catalog involved, no transaction must be opened.
        assert(catalog.lastTransaction === initialLastTxn)
      } finally {
        query.stop()
      }
    }
  }
}

/**
 * A no-op V2 streaming sink with no catalog or identifier (anonymous sink).
 * Used to verify that anonymous V2 sinks do not open transactions.
 */
class NoOpV2SinkProvider extends SimpleTableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new Table with SupportsWrite {
      override def name(): String = "noop-v2-sink"
      override def schema(): StructType = StructType(Nil)
      override def capabilities(): util.Set[TableCapability] =
        util.EnumSet.of(TableCapability.STREAMING_WRITE)
      override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
        new WriteBuilder {
          override def build(): Write = new Write {
            override def toStreaming: StreamingWrite = new StreamingWrite {
              override def createStreamingWriterFactory(
                  info2: PhysicalWriteInfo): StreamingDataWriterFactory = PackedRowWriterFactory
              override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
              override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
            }
          }
        }
    }
  }
}
