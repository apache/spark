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

package org.apache.spark.sql

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.plans.logical.AnalysisBarrier
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class GroupedDatasetSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private val scalaUDF = udf((x: Long) => { x + 1 })
  private lazy val datasetWithUDF = spark.range(1).toDF("s").select($"s", scalaUDF($"s"))

  private def assertContainsAnalysisBarrier(ds: Dataset[_], atLevel: Int = 1): Unit = {
    assert(atLevel >= 0)
    var children = Seq(ds.queryExecution.logical)
    (1 to atLevel).foreach { _ =>
      children = children.flatMap(_.children)
    }
    val barriers = children.collect {
      case ab: AnalysisBarrier => ab
    }
    assert(barriers.nonEmpty, s"Plan does not contain AnalysisBarrier at level $atLevel:\n" +
      ds.queryExecution.logical)
  }

  test("SPARK-24373: avoid running Analyzer rules twice on RelationalGroupedDataset") {
    val groupByDataset = datasetWithUDF.groupBy()
    val rollupDataset = datasetWithUDF.rollup("s")
    val cubeDataset = datasetWithUDF.cube("s")
    val pivotDataset = datasetWithUDF.groupBy().pivot("s", Seq(1, 2))
    datasetWithUDF.cache()
    Seq(groupByDataset, rollupDataset, cubeDataset, pivotDataset).foreach { rgDS =>
      val df = rgDS.count()
      assertContainsAnalysisBarrier(df)
      assertCached(df)
    }

    val flatMapGroupsInRDF = datasetWithUDF.groupBy().flatMapGroupsInR(
      Array.emptyByteArray,
      Array.emptyByteArray,
      Array.empty,
      StructType(Seq(StructField("s", LongType))))
    val flatMapGroupsInPandasDF = datasetWithUDF.groupBy().flatMapGroupsInPandas(PythonUDF(
      "pyUDF",
      null,
      StructType(Seq(StructField("s", LongType))),
      Seq.empty,
      PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
      true))
    Seq(flatMapGroupsInRDF, flatMapGroupsInPandasDF).foreach { df =>
      assertContainsAnalysisBarrier(df, 2)
      assertCached(df)
    }
    datasetWithUDF.unpersist(true)
  }

  test("SPARK-24373: avoid running Analyzer rules twice on KeyValueGroupedDataset") {
    val kvDasaset = datasetWithUDF.groupByKey(_.getLong(0))
    datasetWithUDF.cache()
    val mapValuesKVDataset = kvDasaset.mapValues(_.getLong(0)).reduceGroups(_ + _)
    val keysKVDataset = kvDasaset.keys
    val flatMapGroupsKVDataset = kvDasaset.flatMapGroups((k, _) => Seq(k))
    val aggKVDataset = kvDasaset.count()
    val otherKVDataset = spark.range(1).groupByKey(_ + 1)
    val cogroupKVDataset = kvDasaset.cogroup(otherKVDataset)((k, _, _) => Seq(k))
    Seq((mapValuesKVDataset, 1),
        (keysKVDataset, 2),
        (flatMapGroupsKVDataset, 2),
        (aggKVDataset, 1),
        (cogroupKVDataset, 2)).foreach { case (df, analysisBarrierDepth) =>
      assertContainsAnalysisBarrier(df, analysisBarrierDepth)
      assertCached(df)
    }
    datasetWithUDF.unpersist(true)
  }
}
