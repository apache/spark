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

package org.apache.spark.sql.hive.orc

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.{UnresolvedException, MultiInstanceRelation}
import org.apache.spark.sql.catalyst.expressions.{Expression, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LeafNode}
import org.apache.spark.sql.hive.orc.OrcFileOperator
import org.apache.spark.sql.hive.orc.OrcRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._


private[sql] class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {

  /** Returns a new base relation with the given parameters. */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    OrcRelation(checkPath(parameters), parameters, None)(sqlContext)
  }

  /** Returns a new base relation with the given parameters and schema. */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    OrcRelation(checkPath(parameters), parameters, Some(schema))(sqlContext)
  }

  /** Returns a new base relation with the given parameters and save given data into it. */
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val path = checkPath(parameters)
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doInsertion = (mode, fs.exists(filesystemPath)) match {
      case (SaveMode.ErrorIfExists, true) =>
        sys.error(s"path $path already exists.")
      case (SaveMode.Append, _) | (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
        true
      case (SaveMode.Ignore, exists) =>
        !exists
    }

    val relation = if (doInsertion) {
      val df =
        sqlContext.createDataFrame(
          data.queryExecution.toRdd,
          data.schema.asNullable)
      val createdRelation =
        OrcRelation(checkPath(parameters), parameters, Some(df.schema))(sqlContext)
      createdRelation.insert(df, overwrite = mode == SaveMode.Overwrite)
      createdRelation
    } else {
      // If the save mode is Ignore, we will just create the relation based on existing data.
      OrcRelation(checkPath(parameters), parameters)(sqlContext)
    }
    relation
  }
  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for orc tables."))
  }
}

@DeveloperApi
private[sql] case class OrcRelation(
                                     path: String,
                                     parameters: Map[String, String],
                                     maybeSchema: Option[StructType] = None)(
                                     @transient val sqlContext: SQLContext)
  extends BaseRelation
  with CatalystScan
  with InsertableRelation
  with SparkHadoopMapReduceUtil
  with Logging {
  self: Product =>

  @transient val conf = sqlContext.sparkContext.hadoopConfiguration
  override def schema: StructType =
    maybeSchema.getOrElse(OrcFileOperator.readSchema(path, Some(conf)))
  /** Attributes */
  val output: Seq[Attribute] = schema.toAttributes

  // Equals must also take into account the output attributes so that we can distinguish between
  // different instances of the same relation,
  override def equals(other: Any) = other match {
    case p: OrcRelation =>
      p.path == path && p.output == output
    case _ => false
  }

  override def buildScan(output: Seq[Attribute], predicates: Seq[Expression]): RDD[Row] = {
    OrcTableScan(output, this, predicates).execute()
  }
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    InsertIntoOrcTable(this, data, overwrite).execute()
  }
}

