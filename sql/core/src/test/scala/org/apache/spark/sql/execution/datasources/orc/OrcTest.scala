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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Predicate}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileBasedDataSourceTest}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.orc.OrcTable
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
 *   -> HiveOrcFilterSuite
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
      DataSourceV2Relation(orcTable: OrcTable, _, options)) =>
        assert(filters.nonEmpty, "No filter is analyzed from the given query")
        val scanBuilder = orcTable.newScanBuilder(options)
        scanBuilder.pushFilters(filters.flatMap(DataSourceStrategy.translateFilter).toArray)
        val pushedFilters = scanBuilder.pushedFilters()
        if (noneSupported) {
          assert(pushedFilters.isEmpty, "Unsupported filters should not show in pushed filters")
        } else {
          assert(pushedFilters.nonEmpty, "No filter is pushed down")
          val maybeFilter = OrcFilters.createFilter(query.schema, pushedFilters)
          assert(maybeFilter.isEmpty, s"Couldn't generate filter predicate for $pushedFilters")
        }

      case _ =>
        throw new AnalysisException("Can not match OrcTable in the query.")
    }
  }
}
