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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.toCommentSafeString
import org.apache.spark.sql.execution.datasources.parquet.{DefaultSource => ParquetSource}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, HadoopFsRelation}
import org.apache.spark.sql.types.{AtomicType, DataType}

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
    rdd: RDD[InternalRow])(sqlContext: SQLContext)
  extends LogicalPlan with MultiInstanceRelation {

  override def children: Seq[LogicalPlan] = Nil

  override protected final def otherCopyArgs: Seq[AnyRef] = sqlContext :: Nil

  override def newInstance(): LogicalRDD.this.type =
    LogicalRDD(output.map(_.newInstance()), rdd)(sqlContext).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case LogicalRDD(_, otherRDD) => rdd.id == otherRDD.id
    case _ => false
  }

  override def producedAttributes: AttributeSet = outputSet

  @transient override lazy val statistics: Statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(sqlContext.conf.defaultSizeInBytes)
  )
}

/** Physical plan node for scanning data from an RDD. */
private[sql] case class PhysicalRDD(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    override val nodeName: String) extends LeafNode {

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

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
    s"Scan $nodeName${output.mkString("[", ",", "]")}"
  }
}

private[sql] trait DataSourceScan extends LeafNode {
  val rdd: RDD[InternalRow]
  val relation: BaseRelation

  override val nodeName: String = relation.toString

  // Ignore rdd when checking results
  override def sameResult(plan: SparkPlan): Boolean = plan match {
    case other: DataSourceScan => relation == other.relation && metadata == other.metadata
    case _ => false
  }
}

/** Physical plan node for scanning data from a relation. */
private[sql] case class RowDataSourceScan(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    override val outputPartitioning: Partitioning,
    override val metadata: Map[String, String] = Map.empty)
  extends DataSourceScan with CodegenSupport {

  private[sql] override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  val outputUnsafeRows = relation match {
    case r: HadoopFsRelation if r.fileFormat.isInstanceOf[ParquetSource] =>
      !SQLContext.getActive().get.conf.getConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED)
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
    val metadataEntries = for ((key, value) <- metadata.toSeq.sorted) yield s"$key: $value"
    s"Scan $nodeName${output.mkString("[", ",", "]")}${metadataEntries.mkString(" ", ", ", "")}"
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
private[sql] case class BatchedDataSourceScan(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    override val outputPartitioning: Partitioning,
    override val metadata: Map[String, String] = Map.empty)
  extends DataSourceScan with CodegenSupport {

  private[sql] override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override def simpleString: String = {
    val metadataEntries = for ((key, value) <- metadata.toSeq.sorted) yield s"$key: $value"
    val metadataStr = metadataEntries.mkString(" ", ", ", "")
    s"BatchedScan $nodeName${output.mkString("[", ",", "]")}$metadataStr"
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }

  private def genCodeColumnVector(ctx: CodegenContext, columnVar: String, ordinal: String,
    dataType: DataType, nullable: Boolean): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val value = ctx.getValue(columnVar, dataType, ordinal)
    val isNullVar = if (nullable) { ctx.freshName("isNull") } else { "false" }
    val valueVar = ctx.freshName("value")
    val str = s"columnVector[$columnVar, $ordinal, ${dataType.simpleString}]"
    val code = s"/* ${toCommentSafeString(str)} */\n" + (if (nullable) {
      s"""
        boolean ${isNullVar} = ${columnVar}.isNullAt($ordinal);
        $javaType ${valueVar} = ${isNullVar} ? ${ctx.defaultValue(dataType)} : ($value);
      """
    } else {
      s"$javaType ${valueVar} = $value;"
    }).trim
    ExprCode(code, isNullVar, valueVar)
  }

  // Support codegen so that we can avoid the UnsafeRow conversion in all cases. Codegen
  // never requires UnsafeRow as input.
  override protected def doProduce(ctx: CodegenContext): String = {
    val input = ctx.freshName("input")
    // PhysicalRDD always just has one input
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")

    // metrics
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val scanTimeMetric = metricTerm(ctx, "scanTime")
    val scanTimeTotalNs = ctx.freshName("scanTime")
    ctx.addMutableState("long", scanTimeTotalNs, s"$scanTimeTotalNs = 0;")

    val columnarBatchClz = "org.apache.spark.sql.execution.vectorized.ColumnarBatch"
    val batch = ctx.freshName("batch")
    ctx.addMutableState(columnarBatchClz, batch, s"$batch = null;")

    val columnVectorClz = "org.apache.spark.sql.execution.vectorized.ColumnVector"
    val idx = ctx.freshName("batchIdx")
    ctx.addMutableState("int", idx, s"$idx = 0;")
    val colVars = output.indices.map(i => ctx.freshName("colInstance" + i))
    val columnAssigns = colVars.zipWithIndex.map { case (name, i) =>
      ctx.addMutableState(columnVectorClz, name, s"$name = null;")
      s"$name = $batch.column($i);"
    }

    val nextBatch = ctx.freshName("nextBatch")
    ctx.addNewFunction(nextBatch,
      s"""
         |private void $nextBatch() throws java.io.IOException {
         |  long getBatchStart = System.nanoTime();
         |  if ($input.hasNext()) {
         |    $batch = ($columnarBatchClz)$input.next();
         |    $numOutputRows.add($batch.numRows());
         |    $idx = 0;
         |    ${columnAssigns.mkString("", "\n", "\n")}
         |  }
         |  $scanTimeTotalNs += System.nanoTime() - getBatchStart;
         |}""".stripMargin)

    ctx.currentVars = null
    val rowidx = ctx.freshName("rowIdx")
    val columnsBatchInput = (output zip colVars).map { case (attr, colVar) =>
      genCodeColumnVector(ctx, colVar, rowidx, attr.dataType, attr.nullable)
    }
    s"""
       |if ($batch == null) {
       |  $nextBatch();
       |}
       |while ($batch != null) {
       |  int numRows = $batch.numRows();
       |  while ($idx < numRows) {
       |    int $rowidx = $idx++;
       |    ${consume(ctx, columnsBatchInput).trim}
       |    if (shouldStop()) return;
       |  }
       |  $batch = null;
       |  $nextBatch();
       |}
       |$scanTimeMetric.add($scanTimeTotalNs / (1000 * 1000));
       |$scanTimeTotalNs = 0;
     """.stripMargin
  }
}

private[sql] object DataSourceScan {
  // Metadata keys
  val INPUT_PATHS = "InputPaths"
  val PUSHED_FILTERS = "PushedFilters"

  def create(
      output: Seq[Attribute],
      rdd: RDD[InternalRow],
      relation: BaseRelation,
      metadata: Map[String, String] = Map.empty): DataSourceScan = {
    val outputPartitioning = {
      val bucketSpec = relation match {
        // TODO: this should be closer to bucket planning.
        case r: HadoopFsRelation if r.sqlContext.conf.bucketingEnabled => r.bucketSpec
        case _ => None
      }

      def toAttribute(colName: String): Attribute = output.find(_.name == colName).getOrElse {
        throw new AnalysisException(s"bucket column $colName not found in existing columns " +
          s"(${output.map(_.name).mkString(", ")})")
      }

      bucketSpec.map { spec =>
        val numBuckets = spec.numBuckets
        val bucketColumns = spec.bucketColumnNames.map(toAttribute)
        HashPartitioning(bucketColumns, numBuckets)
      }.getOrElse {
        UnknownPartitioning(0)
      }
    }

    relation match {
      case r: HadoopFsRelation if r.fileFormat.supportBatch(r.sqlContext, relation.schema) =>
        BatchedDataSourceScan(output, rdd, relation, outputPartitioning, metadata)
      case _ =>
        RowDataSourceScan(output, rdd, relation, outputPartitioning, metadata)
    }
  }
}
