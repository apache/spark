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

package org.apache.spark.sql.execution.datasources.orc

import java.io.File

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Predicate}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.FileBasedDataSourceTest
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ORC_IMPLEMENTATION

/**
 * OrcTest
 *   -> OrcSuite
 *       -> OrcSourceSuite
 *       -> HiveOrcSourceSuite
 *   -> OrcQueryTests
 *       -> OrcQuerySuite
 *       -> HiveOrcQuerySuite
 *   -> OrcPartitionDiscoveryTest
 *       -> OrcPartitionDiscoverySuite
 *       -> HiveOrcPartitionDiscoverySuite
 *   -> OrcFilterSuite
 */
abstract class OrcTest extends QueryTest with FileBasedDataSourceTest with BeforeAndAfterAll {

  val orcImp: String = "native"

  private var originalConfORCImplementation = "native"

  override protected val dataSourceName: String = "orc"
  override protected val vectorizedReaderEnabledKey: String =
    SQLConf.ORC_VECTORIZED_READER_ENABLED.key

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    originalConfORCImplementation = spark.conf.get(ORC_IMPLEMENTATION)
    spark.conf.set(ORC_IMPLEMENTATION.key, orcImp)
  }

  protected override def afterAll(): Unit = {
    spark.conf.set(ORC_IMPLEMENTATION.key, originalConfORCImplementation)
    super.afterAll()
  }

  /**
   * Writes `data` to a Orc file, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withOrcFile[T <: Product: ClassTag: TypeTag]
      (data: Seq[T])
      (f: String => Unit): Unit = withDataSourceFile(data)(f)

  /**
   * Writes `data` to a Orc file and reads it back as a `DataFrame`,
   * which is then passed to `f`. The Orc file will be deleted after `f` returns.
   */
  protected def withOrcDataFrame[T <: Product: ClassTag: TypeTag]
      (data: Seq[T], testVectorized: Boolean = true)
      (f: DataFrame => Unit): Unit = withDataSourceDataFrame(data, testVectorized)(f)

  /**
   * Writes `data` to a Orc file, reads it back as a `DataFrame` and registers it as a
   * temporary table named `tableName`, then call `f`. The temporary table together with the
   * Orc file will be dropped/deleted after `f` returns.
   */
  protected def withOrcTable[T <: Product: ClassTag: TypeTag]
      (data: Seq[T], tableName: String, testVectorized: Boolean = true)
      (f: => Unit): Unit = withDataSourceTable(data, tableName, testVectorized)(f)

  protected def makeOrcFile[T <: Product: ClassTag: TypeTag](
      data: Seq[T], path: File): Unit = makeDataSourceFile(data, path)

  protected def makeOrcFile[T <: Product: ClassTag: TypeTag](
      df: DataFrame, path: File): Unit = makeDataSourceFile(df, path)

  protected def checkPredicatePushDown(df: DataFrame, numRows: Int, predicate: String): Unit = {
    withTempPath { file =>
      // It needs to repartition data so that we can have several ORC files
      // in order to skip stripes in ORC.
      df.repartition(numRows).write.orc(file.getCanonicalPath)
      val actual = stripSparkFilter(spark.read.orc(file.getCanonicalPath).where(predicate)).count()
      assert(actual < numRows)
    }
  }

  protected def checkNoFilterPredicate
      (predicate: Predicate, noneSupported: Boolean = false)
      (implicit df: DataFrame): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct
    val query = df
      .select(output.map(e => Column(e)): _*)
      .where(Column(predicate))

    query.queryExecution.optimizedPlan match {
      case PhysicalOperation(_, filters,
          DataSourceV2ScanRelation(_, o: OrcScan, _)) =>
        assert(filters.nonEmpty, "No filter is analyzed from the given query")
        if (noneSupported) {
          assert(o.pushedFilters.isEmpty, "Unsupported filters should not show in pushed filters")
        } else {
          assert(o.pushedFilters.nonEmpty, "No filter is pushed down")
          val maybeFilter = OrcFilters.createFilter(query.schema, o.pushedFilters)
          assert(maybeFilter.isEmpty, s"Couldn't generate filter predicate for ${o.pushedFilters}")
        }

      case _ =>
        throw new AnalysisException("Can not match OrcTable in the query.")
    }
  }

  protected def readResourceOrcFile(name: String): DataFrame = {
    val url = Thread.currentThread().getContextClassLoader.getResource(name)
    // Copy to avoid URISyntaxException when `sql/hive` accesses the resources in `sql/core`
    val file = File.createTempFile("orc-test", ".orc")
    file.deleteOnExit();
    FileUtils.copyURLToFile(url, file)
    spark.read.orc(file.getAbsolutePath)
  }

  def withAllOrcReaders(code: => Unit): Unit = {
    // test the row-based reader
    withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false")(code)
    // test the vectorized reader
    withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "true")(code)
  }

  /**
   * Takes a sequence of products `data` to generate multi-level nested
   * dataframes as new test data. It tests both non-nested and nested dataframes
   * which are written and read back with Orc datasource.
   *
   * This is different from [[withOrcDataFrame]] which does not
   * test nested cases.
   */
  protected def withNestedOrcDataFrame[T <: Product: ClassTag: TypeTag](data: Seq[T])
      (runTest: (DataFrame, String, Any => Any) => Unit): Unit =
    withNestedOrcDataFrame(spark.createDataFrame(data))(runTest)

  protected def withNestedOrcDataFrame(inputDF: DataFrame)
      (runTest: (DataFrame, String, Any => Any) => Unit): Unit = {
    withNestedDataFrame(inputDF).foreach { case (newDF, colName, resultFun) =>
      withTempPath { file =>
        newDF.write.format(dataSourceName).save(file.getCanonicalPath)
        readFile(file.getCanonicalPath, true) { df => runTest(df, colName, resultFun) }
      }
    }
  }
}
