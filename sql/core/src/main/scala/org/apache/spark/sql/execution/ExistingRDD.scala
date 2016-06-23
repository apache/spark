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

package org.apache.spark.sql.execution

import org.apache.commons.lang.StringUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat => ParquetSource}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.Utils

object RDDConversions {
  def productToRowRdd[A <: Product](data: RDD[A], outputTypes: Seq[DataType]): RDD[InternalRow] = {
    data.mapPartitions { iterator =>
      val numColumns = outputTypes.length
      val mutableRow = new GenericMutableRow(numColumns)
      val converters = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)
      iterator.map { r =>
        var i = 0
        while (i < numColumns) {
          mutableRow(i) = converters(i)(r.productElement(i))
          i += 1
        }

        mutableRow
      }
    }
  }

  /**
   * Convert the objects inside Row into the types Catalyst expected.
   */
  def rowToRowRdd(data: RDD[Row], outputTypes: Seq[DataType]): RDD[InternalRow] = {
    data.mapPartitions { iterator =>
      val numColumns = outputTypes.length
      val mutableRow = new GenericMutableRow(numColumns)
      val converters = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)
      iterator.map { r =>
        var i = 0
        while (i < numColumns) {
          mutableRow(i) = converters(i)(r(i))
          i += 1
        }

        mutableRow
      }
    }
  }
}

/** Logical plan node for scanning data from an RDD. */
private[sql] case class LogicalRDD(
    output: Seq[Attribute],
    rdd: RDD[InternalRow])(session: SparkSession)
  extends LogicalPlan with MultiInstanceRelation {

  override def children: Seq[LogicalPlan] = Nil

  override protected final def otherCopyArgs: Seq[AnyRef] = session :: Nil

  override def newInstance(): LogicalRDD.this.type =
    LogicalRDD(output.map(_.newInstance()), rdd)(session).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan): Boolean = {
    plan.canonicalized match {
      case LogicalRDD(_, otherRDD) => rdd.id == otherRDD.id
      case _ => false
    }
  }

  override protected def stringArgs: Iterator[Any] = Iterator(output)

  override def producedAttributes: AttributeSet = outputSet

  @transient override lazy val statistics: Statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(session.sessionState.conf.defaultSizeInBytes)
  )
}

/** Physical plan node for scanning data from an RDD. */
private[sql] case class RDDScanExec(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    override val nodeName: String) extends LeafExecNode {

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    rdd.mapPartitionsInternal { iter =>
      val proj = UnsafeProjection.create(schema)
      iter.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }

  override def simpleString: String = {
    s"Scan $nodeName${Utils.truncatedString(output, "[", ",", "]")}"
  }
}

private[sql] trait DataSourceScanExec extends LeafExecNode {
  val rdd: RDD[InternalRow]
  val relation: BaseRelation
  val metastoreTableIdentifier: Option[TableIdentifier]

  override val nodeName: String = {
    s"Scan $relation ${metastoreTableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  // Ignore rdd when checking results
  override def sameResult(plan: SparkPlan): Boolean = plan match {
    case other: DataSourceScanExec => relation == other.relation && metadata == other.metadata
    case _ => false
  }
}

/** Physical plan node for scanning data from a relation. */
private[sql] case class RowDataSourceScanExec(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    override val outputPartitioning: Partitioning,
    override val metadata: Map[String, String],
    override val metastoreTableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec with CodegenSupport {

  private[sql] override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  val outputUnsafeRows = relation match {
    case r: HadoopFsRelation if r.fileFormat.isInstanceOf[ParquetSource] =>
      !SparkSession.getActiveSession.get.sessionState.conf.getConf(
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED)
    case _: HadoopFsRelation => true
    case _ => false
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val unsafeRow = if (outputUnsafeRows) {
      rdd
    } else {
      rdd.mapPartitionsInternal { iter =>
        val proj = UnsafeProjection.create(schema)
        iter.map(proj)
      }
    }

    val numOutputRows = longMetric("numOutputRows")
    unsafeRow.map { r =>
      numOutputRows += 1
      r
    }
  }

  override def simpleString: String = {
    val metadataEntries = for ((key, value) <- metadata.toSeq.sorted) yield {
      key + ": " + StringUtils.abbreviate(value, 100)
    }

    s"$nodeName${Utils.truncatedString(output, "[", ",", "]")}" +
      s"${Utils.truncatedString(metadataEntries, " ", ", ", "")}"
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    // PhysicalRDD always just has one input
    val input = ctx.freshName("input")
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val exprRows = output.zipWithIndex.map{ case (a, i) =>
      new BoundReference(i, a.dataType, a.nullable)
    }
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsRowInput = exprRows.map(_.genCode(ctx))
    val inputRow = if (outputUnsafeRows) row else null
    s"""
       |while ($input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  $numOutputRows.add(1);
       |  ${consume(ctx, columnsRowInput, inputRow).trim}
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }
}

/** Physical plan node for scanning data from a batched relation. */
private[sql] case class BatchedDataSourceScanExec(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    override val outputPartitioning: Partitioning,
    override val metadata: Map[String, String],
    override val metastoreTableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec with ColumnarBatchScan {

  protected override def doExecute(): RDD[InternalRow] = {
    // in the case of fallback, this batched scan should never fail because of:
    // 1) only primitive types are supported
    // 2) the number of columns should be smaller than spark.sql.codegen.maxFields
    WholeStageCodegenExec(this).execute()
  }

  override def simpleString: String = {
    val metadataEntries = for ((key, value) <- metadata.toSeq.sorted) yield {
      key + ": " + StringUtils.abbreviate(value, 100)
    }
    val metadataStr = Utils.truncatedString(metadataEntries, " ", ", ", "")
    s"Batched$nodeName${Utils.truncatedString(output, "[", ",", "]")}$metadataStr"
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }
}

private[sql] object DataSourceScanExec {
  // Metadata keys
  val INPUT_PATHS = "InputPaths"
  val PUSHED_FILTERS = "PushedFilters"

  def create(
      output: Seq[Attribute],
      rdd: RDD[InternalRow],
      relation: BaseRelation,
      metadata: Map[String, String] = Map.empty,
      metastoreTableIdentifier: Option[TableIdentifier] = None): DataSourceScanExec = {
    val outputPartitioning = {
      val bucketSpec = relation match {
        // TODO: this should be closer to bucket planning.
        case r: HadoopFsRelation
          if r.sparkSession.sessionState.conf.bucketingEnabled => r.bucketSpec
        case _ => None
      }

      bucketSpec.map { spec =>
        val numBuckets = spec.numBuckets
        val bucketColumns = spec.bucketColumnNames.flatMap { n => output.find(_.name == n) }
        if (bucketColumns.size == spec.bucketColumnNames.size) {
          HashPartitioning(bucketColumns, numBuckets)
        } else {
          UnknownPartitioning(0)
        }
      }.getOrElse {
        UnknownPartitioning(0)
      }
    }

    relation match {
      case r: HadoopFsRelation
        if r.fileFormat.supportBatch(r.sparkSession, StructType.fromAttributes(output)) =>
        BatchedDataSourceScanExec(
          output, rdd, relation, outputPartitioning, metadata, metastoreTableIdentifier)
      case _ =>
        RowDataSourceScanExec(
          output, rdd, relation, outputPartitioning, metadata, metastoreTableIdentifier)
    }
  }
}
