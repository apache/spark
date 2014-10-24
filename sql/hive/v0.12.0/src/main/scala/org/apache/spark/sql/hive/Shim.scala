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

package org.apache.spark.sql.hive

import java.net.URI
import java.util.{ArrayList => JArrayList}
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table}
import org.apache.hadoop.hive.ql.plan.{FileSinkDesc, TableDesc}
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.stats.StatsSetupConst
import org.apache.hadoop.hive.serde2.{Deserializer, ColumnProjectionUtils}
import org.apache.hadoop.{io => hadoopIo}
import org.apache.hadoop.mapred.InputFormat
import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * A compatibility layer for interacting with Hive version 0.12.0.
 */
private[hive] object HiveShim {
  val version = "0.12.0"
  val metastoreDecimal = "decimal"

  def getTableDesc(
    serdeClass: Class[_ <: Deserializer],
    inputFormatClass: Class[_ <: InputFormat[_, _]],
    outputFormatClass: Class[_],
    properties: Properties) = {
    new TableDesc(serdeClass, inputFormatClass, outputFormatClass, properties)
  }

  def createDriverResultsArray = new JArrayList[String]

  def processResults(results: JArrayList[String]) = results

  def getStatsSetupConstTotalSize = StatsSetupConst.TOTAL_SIZE

  def createDefaultDBIfNeeded(context: HiveContext) = {  }

  /** The string used to denote an empty comments field in the schema. */
  def getEmptyCommentsFieldValue = "None"

  def getCommandProcessor(cmd: Array[String], conf: HiveConf) = {
    CommandProcessorFactory.get(cmd(0), conf)
  }

  def createDecimal(bd: java.math.BigDecimal): HiveDecimal = {
    new HiveDecimal(bd)
  }

  def appendReadColumns(conf: Configuration, ids: Seq[Integer], names: Seq[String]) {
    ColumnProjectionUtils.appendReadColumnIDs(conf, ids)
    ColumnProjectionUtils.appendReadColumnNames(conf, names)
  }

  def getExternalTmpPath(context: Context, uri: URI) = {
    context.getExternalTmpFileURI(uri)
  }

  def getDataLocationPath(p: Partition) = p.getPartitionPath

  def getAllPartitionsOf(client: Hive, tbl: Table) =  client.getAllPartitionsForPruner(tbl)

}

class ShimFileSinkDesc(var dir: String, var tableInfo: TableDesc, var compressed: Boolean)
  extends FileSinkDesc(dir, tableInfo, compressed) {
}
