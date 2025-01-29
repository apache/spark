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

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{LOGICAL_PLAN_COLUMNS, OPTIMIZED_PLAN_COLUMNS}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, PartitioningCollection, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.classic.{Dataset, SparkSession}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.collection.Utils

object ExternalRDD {

  def apply[T: Encoder](rdd: RDD[T], session: SparkSession): LogicalPlan = {
    val externalRdd = ExternalRDD(CatalystSerde.generateObjAttr[T], rdd)(session)
    CatalystSerde.serialize[T](externalRdd)
  }
}

/** Logical plan node for scanning data from an RDD. */
case class ExternalRDD[T](
    outputObjAttr: Attribute,
    rdd: RDD[T])(session: SparkSession)
  extends LeafNode with ObjectProducer with MultiInstanceRelation {

  override protected final def otherCopyArgs: Seq[AnyRef] = session :: Nil

  override def newInstance(): ExternalRDD.this.type =
    ExternalRDD(outputObjAttr.newInstance(), rdd)(session).asInstanceOf[this.type]

  override protected def stringArgs: Iterator[Any] = Iterator(output)

  override def computeStats(): Statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(session.sessionState.conf.defaultSizeInBytes)
  )
}

/** Physical plan node for scanning data from an RDD. */
case class ExternalRDDScanExec[T](
    outputObjAttr: Attribute,
    rdd: RDD[T]) extends LeafExecNode with ObjectProducerExec {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private def rddName: String = Option(rdd.name).map(n => s" $n").getOrElse("")

  override val nodeName: String = s"Scan$rddName"

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    rdd.mapPartitionsInternal { iter =>
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjectType)
      iter.map { value =>
        numOutputRows += 1
        outputObject(value)
      }
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"$nodeName${truncatedString(output, "[", ", ", "]", maxFields)}"
  }
}

/**
 * Logical plan node for scanning data from an RDD of InternalRow.
 *
 * It is advised to set the field `originStats` and `originConstraints` if the RDD is directly
 * built from DataFrame, so that Spark can make better optimizations.
 */
case class LogicalRDD(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    outputPartitioning: Partitioning = UnknownPartitioning(0),
    override val outputOrdering: Seq[SortOrder] = Nil,
    override val isStreaming: Boolean = false,
    @transient stream: Option[SparkDataStream] = None)(
    session: SparkSession,
    // originStats and originConstraints are intentionally placed to "second" parameter list,
    // to prevent catalyst rules to mistakenly transform and rewrite them. Do not change this.
    originStats: Option[Statistics] = None,
    originConstraints: Option[ExpressionSet] = None)
  extends LeafNode
  with StreamSourceAwareLogicalPlan
  with MultiInstanceRelation {

  import LogicalRDD._

  override protected final def otherCopyArgs: Seq[AnyRef] =
    session :: originStats :: originConstraints :: Nil

  override def newInstance(): LogicalRDD.this.type = {
    val rewrite = Utils.toMap(output, output.map(_.newInstance()))

    val rewrittenPartitioning = outputPartitioning match {
      case p: Expression =>
        p.transform {
          case e: Attribute => rewrite.getOrElse(e, e)
        }.asInstanceOf[Partitioning]

      case p => p
    }

    val rewrittenOrdering = outputOrdering.map(_.transform {
      case e: Attribute => rewrite.getOrElse(e, e)
    }.asInstanceOf[SortOrder])

    val rewrittenStatistics = originStats.map(rewriteStatistics(_, rewrite))
    val rewrittenConstraints = originConstraints.map(rewriteConstraints(_, rewrite))

    LogicalRDD(
      output.map(rewrite),
      rdd,
      rewrittenPartitioning,
      rewrittenOrdering,
      isStreaming,
      stream
    )(session, rewrittenStatistics, rewrittenConstraints).asInstanceOf[this.type]
  }

  override protected def stringArgs: Iterator[Any] = Iterator(output, isStreaming)

  override def computeStats(): Statistics = {
    originStats.getOrElse {
      Statistics(
        // TODO: Instead of returning a default value here, find a way to return a meaningful size
        // estimate for RDDs. See PR 1238 for more discussions.
        sizeInBytes = BigInt(session.sessionState.conf.defaultSizeInBytes)
      )
    }
  }

  override lazy val constraints: ExpressionSet = originConstraints.getOrElse(ExpressionSet())
    // Subqueries can have non-deterministic results even when they only contain deterministic
    // expressions (e.g. consider a LIMIT 1 subquery without an ORDER BY). Propagating predicates
    // containing a subquery causes the subquery to be executed twice (as the result of the subquery
    // in the checkpoint computation cannot be reused), which could result in incorrect results.
    // Therefore we assume that all subqueries are non-deterministic, and we do not expose any
    // constraints that contain a subquery.
    .filterNot(SubqueryExpression.hasSubquery)

  override def withStream(stream: SparkDataStream): LogicalRDD = {
    copy(stream = Some(stream))(session, originStats, originConstraints)
  }

  override def getStream: Option[SparkDataStream] = stream

}

object LogicalRDD extends Logging {
  /**
   * Create a new LogicalRDD based on existing Dataset. Stats and constraints are inherited from
   * origin Dataset.
   */
  private[sql] def fromDataset(
      rdd: RDD[InternalRow],
      originDataset: Dataset[_],
      isStreaming: Boolean): LogicalRDD = {
    // Takes the first leaf partitioning whenever we see a `PartitioningCollection`. Otherwise the
    // size of `PartitioningCollection` may grow exponentially for queries involving deep inner
    // joins.
    @scala.annotation.tailrec
    def firstLeafPartitioning(partitioning: Partitioning): Partitioning = {
      partitioning match {
        case p: PartitioningCollection => firstLeafPartitioning(p.partitionings.head)
        case p => p
      }
    }

    val logicalPlan = originDataset.logicalPlan
    val optimizedPlan = originDataset.queryExecution.optimizedPlan
    val executedPlan = originDataset.queryExecution.executedPlan

    val (stats, constraints) = rewriteStatsAndConstraints(logicalPlan, optimizedPlan)

    LogicalRDD(
      originDataset.logicalPlan.output,
      rdd,
      firstLeafPartitioning(executedPlan.outputPartitioning),
      executedPlan.outputOrdering,
      isStreaming,
      None
    )(originDataset.sparkSession, stats, constraints)
  }

  private[sql] def buildOutputAssocForRewrite(
      source: Seq[Attribute],
      destination: Seq[Attribute]): Option[Map[Attribute, Attribute]] = {
    // We check the name and type, allowing nullability, exprId, metadata, qualifier be different
    // E.g. This could happen during optimization phase.
    val rewrite = source.zip(destination).flatMap { case (attr1, attr2) =>
      if (attr1.name == attr2.name && attr1.dataType == attr2.dataType) {
        Some(attr1 -> attr2)
      } else {
        None
      }
    }.toMap

    if (rewrite.size == source.size) {
      Some(rewrite)
    } else {
      None
    }
  }

  private[sql] def rewriteStatsAndConstraints(
      logicalPlan: LogicalPlan,
      optimizedPlan: LogicalPlan): (Option[Statistics], Option[ExpressionSet]) = {
    val rewrite = buildOutputAssocForRewrite(optimizedPlan.output, logicalPlan.output)

    rewrite.map { rw =>
      val rewrittenStatistics = rewriteStatistics(optimizedPlan.stats, rw)
      val rewrittenConstraints = rewriteConstraints(optimizedPlan.constraints, rw)

      (Some(rewrittenStatistics), Some(rewrittenConstraints))
    }.getOrElse {
      // can't rewrite stats and constraints, give up
      logWarning(log"The output columns are expected to the same (for name and type) for output " +
        log"between logical plan and optimized plan, but they aren't. output in logical plan: " +
        log"${MDC(LOGICAL_PLAN_COLUMNS, logicalPlan.output.map(_.simpleString(10)))} " +
        log"/ output in optimized plan: " +
        log"${MDC(OPTIMIZED_PLAN_COLUMNS, optimizedPlan.output.map(_.simpleString(10)))}")

      (None, None)
    }
  }

  private[sql] def rewriteStatistics(
      originStats: Statistics,
      colRewrite: Map[Attribute, Attribute]): Statistics = {
    Statistics(
      originStats.sizeInBytes,
      originStats.rowCount,
      AttributeMap[ColumnStat](originStats.attributeStats.map {
        case (attr, v) => (colRewrite.getOrElse(attr, attr), v)
      }),
      originStats.isRuntime)
  }

  private[sql] def rewriteConstraints(
      originConstraints: ExpressionSet,
      colRewrite: Map[Attribute, Attribute]): ExpressionSet = {
    originConstraints.map(_.transform {
      case e: Attribute => colRewrite.getOrElse(e, e)
    })
  }
}

/** Physical plan node for scanning data from an RDD of InternalRow. */
case class RDDScanExec(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    name: String,
    override val outputPartitioning: Partitioning = UnknownPartitioning(0),
    override val outputOrdering: Seq[SortOrder] = Nil,
    @transient stream: Option[SparkDataStream] = None)
  extends LeafExecNode
  with StreamSourceAwareSparkPlan
  with InputRDDCodegen {

  private def rddName: String = Option(rdd.name).map(n => s" $n").getOrElse("")

  override val nodeName: String = s"Scan $name$rddName"

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    rdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"$nodeName${truncatedString(output, "[", ",", "]", maxFields)}"
  }

  // Input can be InternalRow, has to be turned into UnsafeRows.
  override protected val createUnsafeProjection: Boolean = true

  override def inputRDD: RDD[InternalRow] = rdd

  // Don't care about `stream` when canonicalizing.
  override protected def doCanonicalize(): SparkPlan = {
    super.doCanonicalize().asInstanceOf[RDDScanExec].copy(stream = None)
  }

  override def getStream: Option[SparkDataStream] = stream
}
