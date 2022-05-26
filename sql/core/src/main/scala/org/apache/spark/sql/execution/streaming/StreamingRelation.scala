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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{ExposesMetadataColumns, LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat}

object StreamingRelation {
  def apply(dataSource: DataSource): StreamingRelation = {
    StreamingRelation(
      dataSource, dataSource.sourceInfo.name, dataSource.sourceInfo.schema.toAttributes)
  }
}

/**
 * Used to link a streaming [[DataSource]] into a
 * [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]]. This is only used for creating
 * a streaming [[org.apache.spark.sql.DataFrame]] from [[org.apache.spark.sql.DataFrameReader]].
 * It should be used to create [[Source]] and converted to [[StreamingExecutionRelation]] when
 * passing to [[StreamExecution]] to run a query.
 */
case class StreamingRelation(dataSource: DataSource, sourceName: String, output: Seq[Attribute])
  extends LeafNode with MultiInstanceRelation with ExposesMetadataColumns {
  override def isStreaming: Boolean = true
  override def toString: String = sourceName

  // There's no sensible value here. On the execution path, this relation will be
  // swapped out with microbatches. But some dataframe operations (in particular explain) do lead
  // to this node surviving analysis. So we satisfy the LeafNode contract with the session default
  // value.
  override def computeStats(): Statistics = Statistics(
    sizeInBytes = BigInt(dataSource.sparkSession.sessionState.conf.defaultSizeInBytes)
  )

  override def newInstance(): LogicalPlan = this.copy(output = output.map(_.newInstance()))

  override lazy val metadataOutput: Seq[AttributeReference] = {
    dataSource.providingClass match {
      // If the dataSource provided class is a same or subclass of FileFormat class
      case f if classOf[FileFormat].isAssignableFrom(f) =>
        val resolve = conf.resolver
        val outputNames = outputSet.map(_.name)
        def isOutputColumn(col: AttributeReference): Boolean = {
          outputNames.exists(name => resolve(col.name, name))
        }
        // filter out the metadata struct column if it has the name conflicting with output columns.
        // if the file has a column "_metadata",
        // then the data column should be returned not the metadata struct column
        Seq(FileFormat.createFileMetadataCol).filterNot(isOutputColumn)
      case _ => Nil
    }
  }

  override def withMetadataColumns(): LogicalPlan = {
    if (metadataOutput.nonEmpty) {
      this.copy(output = output ++ metadataOutput)
    } else {
      this
    }
  }
}

/**
 * Used to link a streaming [[Source]] of data into a
 * [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]].
 */
case class StreamingExecutionRelation(
    source: SparkDataStream,
    output: Seq[Attribute])(session: SparkSession)
  extends LeafNode with MultiInstanceRelation {

  override def otherCopyArgs: Seq[AnyRef] = session :: Nil
  override def isStreaming: Boolean = true
  override def toString: String = source.toString

  // There's no sensible value here. On the execution path, this relation will be
  // swapped out with microbatches. But some dataframe operations (in particular explain) do lead
  // to this node surviving analysis. So we satisfy the LeafNode contract with the session default
  // value.
  override def computeStats(): Statistics = Statistics(
    sizeInBytes = BigInt(session.sessionState.conf.defaultSizeInBytes)
  )

  override def newInstance(): LogicalPlan = this.copy(output = output.map(_.newInstance()))(session)
}

/**
 * A dummy physical plan for [[StreamingRelation]] to support
 * [[org.apache.spark.sql.Dataset.explain]]
 */
case class StreamingRelationExec(sourceName: String, output: Seq[Attribute]) extends LeafExecNode {
  override def toString: String = sourceName
  override protected def doExecute(): RDD[InternalRow] = {
    throw QueryExecutionErrors.cannotExecuteStreamingRelationExecError()
  }
}

object StreamingExecutionRelation {
  def apply(source: Source, session: SparkSession): StreamingExecutionRelation = {
    StreamingExecutionRelation(source, source.schema.toAttributes)(session)
  }
}
