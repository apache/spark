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

package org.apache.spark.sql.collation

import org.apache.parquet.schema.MessageType
import org.apache.spark.SparkConf

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, HadoopFsRelation, LogicalRelation, RelationAndCatalogTable}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.sources.{EqualTo, Filter, IsNotNull}
import org.apache.spark.sql.test.SharedSparkSession

abstract class CollatedFilterPushDownToParquetSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  val dataSource = "parquet"
  val nonCollatedCol = "c0"
  val collatedCol = "c1"
  val collatedStructCol = "c2"
  val collatedStructNestedCol = "f1"
  val collatedStructFieldAccess = s"$collatedStructCol.$collatedStructNestedCol"
  val collatedArrayCol = "c3"
  val collatedMapCol = "c4"

  val lcaseCollation = "'UTF8_LCASE'"

  def getPushedDownFilters(query: DataFrame): Seq[Filter]

  protected def createParquetFilters(schema: MessageType): ParquetFilters =
    new ParquetFilters(schema, conf.parquetFilterPushDownDate, conf.parquetFilterPushDownTimestamp,
      conf.parquetFilterPushDownDecimal, conf.parquetFilterPushDownStringPredicate,
      conf.parquetFilterPushDownInFilterThreshold,
      conf.caseSensitiveAnalysis,
      RebaseSpec(LegacyBehaviorPolicy.CORRECTED))

  def testPushDown(
      filterString: String,
      expectedPushedFilters: Seq[Filter],
      expectedRowCount: Int): Unit = {
    withTempPath { path =>
      val df = sql(
        s"""
           |SELECT
           |  c as $nonCollatedCol,
           |  COLLATE(c, $lcaseCollation) as $collatedCol,
           |  named_struct('$collatedStructNestedCol',
           |    COLLATE(c, $lcaseCollation)) as $collatedStructCol,
           |  array(COLLATE(c, $lcaseCollation)) as $collatedArrayCol,
           |  map(COLLATE(c, $lcaseCollation), 1) as $collatedMapCol
           |FROM VALUES ('aaa'), ('AAA'), ('bbb')
           |as data(c)
           |""".stripMargin)

      df.write.format(dataSource).save(path.getAbsolutePath)

      val query = spark.read.format(dataSource).load(path.getAbsolutePath)
        .filter(filterString)

      val actualPushedFilters = getPushedDownFilters(query)
      assert(actualPushedFilters.toSet === expectedPushedFilters.toSet)
      assert(query.count() === expectedRowCount)
    }
  }

  test("do not push down anything for literal comparison") {
    testPushDown(
       filterString = s"'aaa' COLLATE UNICODE = 'bbb' COLLATE UNICODE",
       expectedPushedFilters = Seq.empty,
       expectedRowCount = 0)
  }

  test("push down null check for collated column") {
    testPushDown(
      filterString = s"$collatedCol = 'aaa'",
      expectedPushedFilters = Seq(IsNotNull(collatedCol)),
      expectedRowCount = 2)
  }

  test("push down null check for non-equality check") {
    testPushDown(
      filterString = s"$collatedCol != 'aaa'",
      expectedPushedFilters = Seq(IsNotNull(collatedCol)),
      expectedRowCount = 1)
  }

  test("push down null check for greater than check") {
    testPushDown(
      filterString = s"$collatedCol > 'aaa'",
      expectedPushedFilters = Seq(IsNotNull(collatedCol)),
      expectedRowCount = 1)
  }

  test("push down null check for gte check") {
    testPushDown(
      filterString = s"$collatedCol >= 'aaa'",
      expectedPushedFilters = Seq(IsNotNull(collatedCol)),
      expectedRowCount = 3)
  }

  test("push down null check for less than check") {
    testPushDown(
      filterString = s"$collatedCol < 'aaa'",
      expectedPushedFilters = Seq(IsNotNull(collatedCol)),
      expectedRowCount = 0)
  }

  test("push down null check for lte check") {
    testPushDown(
      filterString = s"$collatedCol <= 'aaa'",
      expectedPushedFilters = Seq(IsNotNull(collatedCol)),
      expectedRowCount = 2)
  }

  test("push down null check for STARTSWITH") {
    testPushDown(
      filterString = s"STARTSWITH($collatedCol, 'a')",
      expectedPushedFilters = Seq(IsNotNull(collatedCol)),
      expectedRowCount = 2)
  }

  test("push down null check for ENDSWITH") {
    testPushDown(
      filterString = s"ENDSWITH($collatedCol, 'a')",
      expectedPushedFilters = Seq(IsNotNull(collatedCol)),
      expectedRowCount = 2)
  }

  test("push down null check for CONTAINS") {
    testPushDown(
      filterString = s"CONTAINS($collatedCol, 'a')",
      expectedPushedFilters = Seq(IsNotNull(collatedCol)),
      expectedRowCount = 2)
  }

  test("no push down for IN") {
    testPushDown(
      filterString = s"$collatedCol IN ('aaa', 'bbb')",
      expectedPushedFilters = Seq.empty,
      expectedRowCount = 3)
  }

  test("push down null check for equality for non-collated column in AND") {
    testPushDown(
      filterString = s"$collatedCol = 'aaa' AND $nonCollatedCol = 'aaa'",
      expectedPushedFilters =
        Seq(IsNotNull(collatedCol), IsNotNull(nonCollatedCol), EqualTo(nonCollatedCol, "aaa")),
      expectedRowCount = 1)
  }

  test("for OR do not push down anything") {
    testPushDown(
      filterString = s"$collatedCol = 'aaa' OR $nonCollatedCol = 'aaa'",
      expectedPushedFilters = Seq.empty,
      expectedRowCount = 2)
  }

  test("mix OR and AND") {
    testPushDown(
      filterString = s"$collatedCol = 'aaa' AND ($nonCollatedCol = 'aaa' OR $collatedCol = 'aaa')",
      expectedPushedFilters = Seq(IsNotNull(collatedCol)),
      expectedRowCount = 2)
  }

  test("negate check on collated column") {
    testPushDown(
      filterString = s"NOT($collatedCol == 'aaa')",
      expectedPushedFilters = Seq(IsNotNull(collatedCol)),
      expectedRowCount = 1)
  }

  test("compare entire struct - parquet does not support null check on complex types") {
    testPushDown(
      filterString = s"$collatedStructCol = " +
        s"named_struct('$collatedStructNestedCol', collate('aaa', $lcaseCollation))",
      expectedPushedFilters = Seq.empty,
      expectedRowCount = 2)
  }

  test("inner struct field access") {
    testPushDown(
      filterString = s"$collatedStructFieldAccess = 'aaa'",
      expectedPushedFilters = Seq(IsNotNull(collatedStructFieldAccess)),
      expectedRowCount = 2)
  }

  test("array - parquet does not support null check on complex types") {
    testPushDown(
      filterString = s"$collatedArrayCol = array(collate('aaa', $lcaseCollation))",
      expectedPushedFilters = Seq.empty,
      expectedRowCount = 2)
  }

  test("map - parquet does not support null check on complex types") {
    testPushDown(
      filterString = s"map_keys($collatedMapCol) != array(collate('aaa', $lcaseCollation))",
      expectedPushedFilters = Seq.empty,
      expectedRowCount = 1)
  }
}

class CollatedFilterPushDownToParquetV1Suite extends CollatedFilterPushDownToParquetSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, dataSource)

  override def getPushedDownFilters(query: DataFrame): Seq[Filter] = {
    var maybeRelation: Option[HadoopFsRelation] = None
    val maybeAnalyzedPredicate = query.queryExecution.optimizedPlan.collect {
      case PhysicalOperation(_, filters,
          RelationAndCatalogTable(_, relation: HadoopFsRelation, _)) =>
        maybeRelation = Some(relation)
        filters
    }.flatten

    if (maybeAnalyzedPredicate.isEmpty) {
      return Seq.empty
    }

    val (_, selectedFilters, _) =
      DataSourceStrategy.selectFilters(maybeRelation.get, maybeAnalyzedPredicate)

    val schema = new SparkToParquetSchemaConverter(conf).convert(query.schema)
    val parquetFilters = createParquetFilters(schema)
    parquetFilters.convertibleFilters(selectedFilters)
  }
}

class CollatedFilterPushDownToParquetV2Suite extends CollatedFilterPushDownToParquetSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  override def getPushedDownFilters(query: DataFrame): Seq[Filter] = {
    query.queryExecution.optimizedPlan.collectFirst {
      case PhysicalOperation(_, _,
          DataSourceV2ScanRelation(_, scan: ParquetScan, _, _, _)) =>
        scan.pushedFilters.toSeq
    }.getOrElse(Seq.empty)
  }
}
