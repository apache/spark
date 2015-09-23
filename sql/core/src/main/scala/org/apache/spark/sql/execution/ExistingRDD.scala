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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericMutableRow}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Row, SQLContext}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
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

  override def newInstance(): LogicalRDD.this.type =
    LogicalRDD(output.map(_.newInstance()), rdd)(sqlContext).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case LogicalRDD(_, otherRDD) => rdd.id == otherRDD.id
    case _ => false
  }

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
    extraInformation: String) extends LeafNode {

  protected override def doExecute(): RDD[InternalRow] = rdd

  override def simpleString: String = "Scan " + extraInformation + output.mkString("[", ",", "]")
}

private[sql] object PhysicalRDD {
  def createFromDataSource(
      output: Seq[Attribute],
      rdd: RDD[InternalRow],
      relation: BaseRelation): PhysicalRDD = {
    PhysicalRDD(output, rdd, relation.toString)
  }
}

/** Logical plan node for scanning data from a local collection. */
private[sql]
case class LogicalLocalTable(output: Seq[Attribute], rows: Seq[InternalRow])(sqlContext: SQLContext)
   extends LogicalPlan with MultiInstanceRelation {

  override def children: Seq[LogicalPlan] = Nil

  override def newInstance(): this.type =
    LogicalLocalTable(output.map(_.newInstance()), rows)(sqlContext).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case LogicalRDD(_, otherRDD) => rows == rows
    case _ => false
  }

  @transient override lazy val statistics: Statistics = Statistics(
    // TODO: Improve the statistics estimation.
    // This is made small enough so it can be broadcasted.
    sizeInBytes = sqlContext.conf.autoBroadcastJoinThreshold - 1
  )
}
