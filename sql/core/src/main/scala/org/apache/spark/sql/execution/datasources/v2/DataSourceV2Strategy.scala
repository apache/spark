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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{sources, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, Repartition}
import org.apache.spark.sql.execution.{FilterExec, LeafExecNode, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.streaming.{ContinuousExecutionRelation, MicroBatchExecutionRelation, StreamingExecutionRelation}
import org.apache.spark.sql.execution.streaming.continuous.{ContinuousCoalesceExec, WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousInputStream, MicroBatchInputStream}

object DataSourceV2Strategy extends Strategy {

  /**
   * Pushes down filters to the data source reader
   *
   * @return pushed filter and post-scan filters.
   */
  private def pushFilters(
      configBuilder: ScanConfigBuilder,
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    configBuilder match {
      case r: SupportsPushDownFilters =>
        // A map from translated data source filters to original catalyst filter expressions.
        val translatedFilterToExpr = mutable.HashMap.empty[sources.Filter, Expression]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated = DataSourceStrategy.translateFilter(filterExpr)
          if (translated.isDefined) {
            translatedFilterToExpr(translated.get) = filterExpr
          } else {
            untranslatableExprs += filterExpr
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushFilters(translatedFilterToExpr.keys.toArray)
          .map(translatedFilterToExpr)
        // The filters which are marked as pushed to this data source
        val pushedFilters = r.pushedFilters().map(translatedFilterToExpr)
        (pushedFilters, untranslatableExprs ++ postScanFilters)

      case _ => (Nil, filters)
    }
  }

  /**
   * Applies column pruning to the data source, w.r.t. the references of the given expressions.
   *
   * @return the created `ScanConfig`(since column pruning is the last step of operator pushdown),
   *         and new output attributes after column pruning.
   */
  // TODO: nested column pruning.
  private def pruneColumns(
      configBuilder: ScanConfigBuilder,
      relation: DataSourceV2Relation,
      exprs: Seq[Expression]): (ScanConfig, Seq[AttributeReference]) = {
    configBuilder match {
      case r: SupportsPushDownRequiredColumns =>
        val requiredColumns = AttributeSet(exprs.flatMap(_.references))
        val neededOutput = relation.output.filter(requiredColumns.contains)
        if (neededOutput != relation.output) {
          r.pruneColumns(neededOutput.toStructType)
          val config = r.build()
          val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
          config -> config.readSchema().toAttributes.map {
            // We have to keep the attribute id during transformation.
            a => a.withExprId(nameToAttr(a.name).exprId)
          }
        } else {
          r.build() -> relation.output
        }

      case _ => configBuilder.build() -> relation.output
    }
  }


  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters, relation: DataSourceV2Relation) =>
      val dsOptions = new DataSourceOptions(relation.options.asJava)
      val configBuilder = relation.table.newScanConfigBuilder(dsOptions)
      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFilters) = pushFilters(configBuilder, filters)
      val (config, output) = pruneColumns(configBuilder, relation, project ++ postScanFilters)
      logInfo(
        s"""
           |Pushing operators to ${relation.source.getClass}
           |Pushed Filters: ${pushedFilters.mkString(", ")}
           |Post-Scan Filters: ${postScanFilters.mkString(",")}
           |Output: ${output.mkString(", ")}
         """.stripMargin)

      val scan = DataSourceV2ScanExec(
        output,
        relation.source,
        relation.options,
        pushedFilters,
        relation.table.createBatchScan(config, dsOptions))

      val filterCondition = postScanFilters.reduceLeftOption(And)
      val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

      // always add the projection, which will produce unsafe rows required by some operators
      ProjectExec(project, withFilter) :: Nil

    // Ideally `StreamingExecutionRelation`, `MicroBatchExecutionRelation` and
    // `ContinuousExecutionRelation` are temporary and we don't need to handle them in strategy
    // rules. However, the current streaming framework keeps a base logical plan instead of physical
    // plan, so we need to do a temp query planning at the beginning to get operator pushdown
    // result. Here we catch these temp logical plans, return fake physical plans to report the
    // operator pushdown result.
    case r: StreamingExecutionRelation =>
      FakeStreamingScanExec(r.output) :: Nil

    case r: MicroBatchExecutionRelation =>
      val options = new DataSourceOptions(r.options.asJava)
      val configBuilder = r.table.newScanConfigBuilder(options)
      // TODO: operator pushdown
      val config = configBuilder.build()
      val stream = r.table.createMicroBatchInputStream(r.metadataPath, config, options)
      FakeMicroBatchExec(r, stream, config.readSchema().toAttributes) :: Nil

    case r: ContinuousExecutionRelation =>
      val options = new DataSourceOptions(r.options.asJava)
      val configBuilder = r.table.newScanConfigBuilder(options)
      // TODO: operator pushdown
      val config = configBuilder.build()
      val stream = r.table.createContinuousInputStream(r.metadataPath, config, options)
      FakeContinuousExec(r, stream, config.readSchema().toAttributes) :: Nil

    case r: StreamingDataSourceV2Relation =>
      // ensure there is a projection, which will produce unsafe rows required by some operators
      ProjectExec(r.output,
        DataSourceV2ScanExec(
          r.output,
          r.source,
          r.options,
          r.pushedFilters,
          r.createScan(),
          Some(r.stream))) :: Nil

    case WriteToDataSourceV2(writer, query) =>
      WriteToDataSourceV2Exec(writer, planLater(query)) :: Nil

    case AppendData(r: DataSourceV2Relation, query, _) =>
      WriteToDataSourceV2Exec(r.newWriteSupport(), planLater(query)) :: Nil

    case WriteToContinuousDataSource(writer, query) =>
      WriteToContinuousDataSourceExec(writer, planLater(query)) :: Nil

    case Repartition(1, false, child) =>
      val isContinuous = child.find {
        case s: StreamingDataSourceV2Relation => s.stream.isInstanceOf[ContinuousInputStream]
        case _ => false
      }.isDefined

      if (isContinuous) {
        ContinuousCoalesceExec(1, planLater(child)) :: Nil
      } else {
        Nil
      }

    case _ => Nil
  }
}

case class FakeStreamingScanExec(output: Seq[Attribute]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("cannot execute FakeStreamingScanExec")
  }
}

case class FakeMicroBatchExec(
    relation: MicroBatchExecutionRelation,
    stream: MicroBatchInputStream,
    output: Seq[Attribute]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("cannot execute FakeMicroBatchExec")
  }
}

case class FakeContinuousExec(
    relation: ContinuousExecutionRelation,
    stream: ContinuousInputStream,
    output: Seq[Attribute]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("cannot execute FakeContinuousExec")
  }
}
