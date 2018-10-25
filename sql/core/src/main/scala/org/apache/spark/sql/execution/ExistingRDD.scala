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
import org.apache.spark.sql.{Encoder, Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.Utils

object RDDConversions {
  def productToRowRdd[A <: Product](data: RDD[A], outputTypes: Seq[DataType]): RDD[InternalRow] = {
    data.mapPartitions { iterator =>
      val numColumns = outputTypes.length
      val mutableRow = new GenericInternalRow(numColumns)
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
      val mutableRow = new GenericInternalRow(numColumns)
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
    val outputDataType = outputObjAttr.dataType
    rdd.mapPartitionsInternal { iter =>
      val outputObject = ObjectOperator.wrapObjectToRow(outputDataType)
      iter.map { value =>
        numOutputRows += 1
        outputObject(value)
      }
    }
  }

  override def simpleString: String = {
    s"$nodeName${output.mkString("[", ",", "]")}"
  }
}

/** Logical plan node for scanning data from an RDD of InternalRow. */
case class LogicalRDD(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    outputPartitioning: Partitioning = UnknownPartitioning(0),
    override val outputOrdering: Seq[SortOrder] = Nil,
    override val isStreaming: Boolean = false)(session: SparkSession)
  extends LeafNode with MultiInstanceRelation {

  override protected final def otherCopyArgs: Seq[AnyRef] = session :: Nil

  override def newInstance(): LogicalRDD.this.type = {
    val rewrite = output.zip(output.map(_.newInstance())).toMap

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

    LogicalRDD(
      output.map(rewrite),
      rdd,
      rewrittenPartitioning,
      rewrittenOrdering,
      isStreaming
    )(session).asInstanceOf[this.type]
  }

  override protected def stringArgs: Iterator[Any] = Iterator(output, isStreaming)

  override def computeStats(): Statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(session.sessionState.conf.defaultSizeInBytes)
  )
}

/** Physical plan node for scanning data from an RDD of InternalRow. */
case class RDDScanExec(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    name: String,
    override val outputPartitioning: Partitioning = UnknownPartitioning(0),
    override val outputOrdering: Seq[SortOrder] = Nil) extends LeafExecNode {

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

  override def simpleString: String = {
    s"$nodeName${Utils.truncatedString(output, "[", ",", "]")}"
  }
}
