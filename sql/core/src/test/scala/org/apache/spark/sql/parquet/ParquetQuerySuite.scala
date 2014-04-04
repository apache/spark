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

package org.apache.spark.sql.parquet

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import parquet.hadoop.ParquetFileWriter
import parquet.hadoop.util.ContextUtil
import parquet.schema.MessageTypeParser

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.util.getTempFilePath
import org.apache.spark.sql.test.TestSQLContext

// Implicits
import org.apache.spark.sql.test.TestSQLContext._

class ParquetQuerySuite extends FunSuite with BeforeAndAfterAll {
  override def beforeAll() {
    ParquetTestData.writeFile()
  }

  override def afterAll() {
    ParquetTestData.testFile.delete()
  }

  test("self-join parquet files") {
    val x = ParquetTestData.testData.subquery('x)
    val y = ParquetTestData.testData.subquery('y)
    val query = x.join(y).where("x.myint".attr === "y.myint".attr)

    // Check to make sure that the attributes from either side of the join have unique expression
    // ids.
    query.queryExecution.analyzed.output.filter(_.name == "myint") match {
      case Seq(i1, i2) if(i1.exprId == i2.exprId) =>
        fail(s"Duplicate expression IDs found in query plan: $query")
      case Seq(_, _) => // All good
    }

    // TODO: We can't run this query as it NPEs
  }

  test("Import of simple Parquet file") {
    val result = getRDD(ParquetTestData.testData).collect()
    assert(result.size === 15)
    result.zipWithIndex.foreach {
      case (row, index) => {
        val checkBoolean =
          if (index % 3 == 0)
            row(0) == true
          else
            row(0) == false
        assert(checkBoolean === true, s"boolean field value in line $index did not match")
        if (index % 5 == 0) assert(row(1) === 5, s"int field value in line $index did not match")
        assert(row(2) === "abc", s"string field value in line $index did not match")
        assert(row(3) === (index.toLong << 33), s"long value in line $index did not match")
        assert(row(4) === 2.5F, s"float field value in line $index did not match")
        assert(row(5) === 4.5D, s"double field value in line $index did not match")
      }
    }
  }

  test("Projection of simple Parquet file") {
    val scanner = new ParquetTableScan(
      ParquetTestData.testData.output,
      ParquetTestData.testData,
      None)(TestSQLContext.sparkContext)
    val projected = scanner.pruneColumns(ParquetTypesConverter
      .convertToAttributes(MessageTypeParser
      .parseMessageType(ParquetTestData.subTestSchema)))
    assert(projected.output.size === 2)
    val result = projected
      .execute()
      .map(_.copy())
      .collect()
    result.zipWithIndex.foreach {
      case (row, index) => {
          if (index % 3 == 0)
            assert(row(0) === true, s"boolean field value in line $index did not match (every third row)")
          else
            assert(row(0) === false, s"boolean field value in line $index did not match")
        assert(row(1) === (index.toLong << 33), s"long field value in line $index did not match")
        assert(row.size === 2, s"number of columns in projection in line $index is incorrect")
      }
    }
  }

  test("Writing metadata from scratch for table CREATE") {
    val job = new Job()
    val path = new Path(getTempFilePath("testtable").getCanonicalFile.toURI.toString)
    val fs: FileSystem = FileSystem.getLocal(ContextUtil.getConfiguration(job))
    ParquetTypesConverter.writeMetaData(
      ParquetTestData.testData.output,
      path,
      TestSQLContext.sparkContext.hadoopConfiguration)
    assert(fs.exists(new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)))
    val metaData = ParquetTypesConverter.readMetaData(path)
    assert(metaData != null)
    ParquetTestData
      .testData
      .parquetSchema
      .checkContains(metaData.getFileMetaData.getSchema) // throws exception if incompatible
    metaData
      .getFileMetaData
      .getSchema
      .checkContains(ParquetTestData.testData.parquetSchema) // throws exception if incompatible
    fs.delete(path, true)
  }

  /**
   * Computes the given [[ParquetRelation]] and returns its RDD.
   *
   * @param parquetRelation The Parquet relation.
   * @return An RDD of Rows.
   */
  private def getRDD(parquetRelation: ParquetRelation): RDD[Row] = {
    val scanner = new ParquetTableScan(
      parquetRelation.output,
      parquetRelation,
      None)(TestSQLContext.sparkContext)
    scanner
      .execute
      .map(_.copy())
  }
}

