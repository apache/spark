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
import org.apache.spark.sql.sources.{HadoopFsRelation, HadoopFsRelationProvider, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

/**
 * A container for bucketing information.
 * Bucketing is a technology for decomposing data sets into more manageable parts, and the number
 * of buckets is fixed so it does not fluctuate with data.
 *
 * @param numBuckets number of buckets.
 * @param bucketColumnNames the names of the columns that used to generate the bucket id.
 * @param sortColumnNames the names of the columns that used to sort data in each bucket.
 */
private[sql] case class BucketSpec(
    numBuckets: Int,
    bucketColumnNames: Seq[String],
    sortColumnNames: Seq[String])

private[sql] trait BucketedHadoopFsRelationProvider extends HadoopFsRelationProvider {
  final override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation =
    // TODO: throw exception here as we won't call this method during execution, after bucketed read
    // support is finished.
    createRelation(sqlContext, paths, dataSchema, partitionColumns, bucketSpec = None, parameters)
}

private[sql] abstract class BucketedOutputWriterFactory extends OutputWriterFactory {
  final override def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter =
    throw new UnsupportedOperationException("use bucket version")
}
