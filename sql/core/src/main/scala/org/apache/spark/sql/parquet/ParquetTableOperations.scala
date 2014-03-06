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
package parquet

import _root_.parquet.io.InvalidRecordException
import _root_.parquet.schema.MessageType
import _root_.parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import _root_.parquet.hadoop.util.ContextUtil

import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.{Row, Attribute, Expression}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode, LeafNode}

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.IOException

/**
 * Parquet table scan operator. Imports the file that backs the given
 * [[ParquetRelation]] as a RDD[Row].
 */
case class ParquetTableScan(
    output: Seq[Attribute],
    relation: ParquetRelation,
    columnPruningPred: Option[Expression])(
    @transient val sc: SparkContext)
  extends LeafNode {

  override def execute(): RDD[Row] = {
    val job = new Job(sc.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(
      job,
      classOf[org.apache.spark.sql.parquet.RowReadSupport])
    val conf: Configuration = ContextUtil.getConfiguration(job)
    conf.set(
        RowReadSupport.PARQUET_ROW_REQUESTED_SCHEMA,
        ParquetTypesConverter.convertFromAttributes(output).toString)
    // TODO: think about adding record filters
    sc.newAPIHadoopFile(
      relation.path,
      classOf[ParquetInputFormat[Row]],
      classOf[Void], classOf[Row],
      conf)
      .map(_._2)
  }

  /**
   * Applies a (candidate) projection.
   *
   * @param prunedAttributes The list of attributes to be used in the projection.
   * @return Pruned TableScan.
   */
  def pruneColumns(prunedAttributes: Seq[Attribute]): ParquetTableScan = {
    val success = validateProjection(prunedAttributes)
    if(success) {
      ParquetTableScan(prunedAttributes, relation, columnPruningPred)(sc)
    } else {
      sys.error("Warning: Could not validate Parquet schema projection in pruneColumns")
      this
    }
  }

  /**
   * Evaluates a candidate projection by checking whether the candidate is a subtype
   * of the original type.
   *
   * @param projection The candidate projection.
   * @return True if the projection is valid, false otherwise.
   */
  private def validateProjection(projection: Seq[Attribute]): Boolean = {
    val original: MessageType = relation.parquetSchema
    val candidate: MessageType = ParquetTypesConverter.convertFromAttributes(projection)
    try {
      original.checkContains(candidate)
      true
    } catch {
      case e: InvalidRecordException => {
        false
      }
    }
  }
}

case class InsertIntoParquetTable(
    relation: ParquetRelation,
    child: SparkPlan)(
    @transient val sc: SparkContext)
  extends UnaryNode {

  /**
   * Inserts all the rows in the Parquet file. Note that OVERWRITE is implicit, since
   * Parquet files are write-once.
   */
  override def execute() = {
    // TODO: currently we do not check whether the "schema"s are compatible
    // That means if one first creates a table and then INSERTs data with
    // and incompatible schema the execution will fail. It would be nice
    // to catch this early one, maybe having the planner validate the schema
    // before calling execute().

    val childRdd = child.execute()
    assert(childRdd != null)

    val job = new Job(sc.hadoopConfiguration)

    ParquetOutputFormat.setWriteSupportClass(
      job,
      classOf[org.apache.spark.sql.parquet.RowWriteSupport])

    // TODO: move that to function in object
    val conf = job.getConfiguration
    conf.set(RowWriteSupport.PARQUET_ROW_SCHEMA, relation.parquetSchema.toString)

    val fspath = new Path(relation.path)
    val fs = fspath.getFileSystem(conf)

    try {
      fs.delete(fspath, true)
    } catch {
      case e: IOException =>
        throw new IOException(
          s"Unable to clear output directory ${fspath.toString} prior"
          + s" to InsertIntoParquetTable:\n${e.toString}")
    }

    JavaPairRDD.fromRDD(childRdd.map(Tuple2(null, _))).saveAsNewAPIHadoopFile(
      relation.path.toString,
      classOf[Void],
      classOf[ParquetRelation.RowType],
      classOf[_root_.parquet.hadoop.ParquetOutputFormat[ParquetRelation.RowType]],
      conf)

    // We return the child RDD to allow chaining (alternatively, one could return nothing).
    childRdd
  }

  override def output = child.output
}

