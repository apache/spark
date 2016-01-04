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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources.{OutputWriter, OutputWriterFactory, HadoopFsRelationProvider, HadoopFsRelation}
import org.apache.spark.sql.types.StructType

private[sql] case class BucketSpec(
    numBuckets: Int,
    bucketingColumns: Seq[String],
    sortingColumns: Option[Seq[String]]) {

  def resolvedBucketingColumns(inputSchema: Seq[Attribute]): Seq[Attribute] = {
    bucketingColumns.map { col =>
      inputSchema.find(_.name == col).get
    }
  }

  def resolvedSortingColumns(inputSchema: Seq[Attribute]): Seq[Attribute] = {
    if (sortingColumns.isDefined) {
      sortingColumns.get.map { col =>
        inputSchema.find(_.name == col).get
      }
    } else {
      Nil
    }
  }
}

private[sql] trait BucketedHadoopFsRelationProvider extends HadoopFsRelationProvider {
  final override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation =
    createRelation(sqlContext, paths, dataSchema, partitionColumns, None, parameters)

  def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      bucketSpec: Option[BucketSpec],
      parameters: Map[String, String]): BucketedHadoopFsRelation
}

private[sql] abstract class BucketedHadoopFsRelation(
    maybePartitionSpec: Option[PartitionSpec],
    parameters: Map[String, String])
  extends HadoopFsRelation(maybePartitionSpec, parameters) {
  def this() = this(None, Map.empty[String, String])

  def this(parameters: Map[String, String]) = this(None, parameters)

  def bucketSpec: Option[BucketSpec]
}

private[sql] abstract class BucketedOutputWriterFactory extends OutputWriterFactory {
  final override def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter =
    newInstance(path, None, dataSchema, context)

  def newInstance(
      path: String,
      bucketId: Option[Int],
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter
}
