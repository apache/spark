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
import org.apache.hadoop.hive.ql.plan.{CreateTableDesc, FileSinkDesc, TableDesc}
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.stats.StatsSetupConst
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.{Deserializer, ColumnProjectionUtils}
import org.apache.hadoop.hive.serde2.{io => hiveIo}
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

  def getPrimitiveWritableConstantObjectInspector(value: String): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.STRING, new hadoopIo.Text(value))

  def getPrimitiveWritableConstantObjectInspector(value: Int): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.INT, new hadoopIo.IntWritable(value))

  def getPrimitiveWritableConstantObjectInspector(value: Double): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.DOUBLE, new hiveIo.DoubleWritable(value))

  def getPrimitiveWritableConstantObjectInspector(value: Boolean): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.BOOLEAN, new hadoopIo.BooleanWritable(value))

  def getPrimitiveWritableConstantObjectInspector(value: Long): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.LONG, new hadoopIo.LongWritable(value))

  def getPrimitiveWritableConstantObjectInspector(value: Float): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.FLOAT, new hadoopIo.FloatWritable(value))

  def getPrimitiveWritableConstantObjectInspector(value: Short): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.SHORT, new hiveIo.ShortWritable(value))

  def getPrimitiveWritableConstantObjectInspector(value: Byte): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.BYTE, new hiveIo.ByteWritable(value))

  def getPrimitiveWritableConstantObjectInspector(value: Array[Byte]): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.BINARY, new hadoopIo.BytesWritable(value))

  def getPrimitiveWritableConstantObjectInspector(value: java.sql.Date): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.DATE, new hiveIo.DateWritable(value))

  def getPrimitiveWritableConstantObjectInspector(value: java.sql.Timestamp): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.TIMESTAMP, new hiveIo.TimestampWritable(value))

  def getPrimitiveWritableConstantObjectInspector(value: BigDecimal): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.DECIMAL,
      new hiveIo.HiveDecimalWritable(HiveShim.createDecimal(value.underlying())))

  def getPrimitiveNullWritableConstantObjectInspector: ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.VOID, null)

  def createDriverResultsArray = new JArrayList[String]

  def processResults(results: JArrayList[String]) = results

  def getStatsSetupConstTotalSize = StatsSetupConst.TOTAL_SIZE

  def createDefaultDBIfNeeded(context: HiveContext) = {  }

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

  def compatibilityBlackList = Seq(
    "decimal_.*",
    "drop_partitions_filter2",
    "show_.*",
    "serde_regex",
    "udf_to_date",
    "udaf_collect_set",
    "udf_concat"
  )

  def setLocation(tbl: Table, crtTbl: CreateTableDesc): Unit = {
    tbl.setDataLocation(new Path(crtTbl.getLocation()).toUri())
  }
}

class ShimFileSinkDesc(var dir: String, var tableInfo: TableDesc, var compressed: Boolean)
  extends FileSinkDesc(dir, tableInfo, compressed) {
}
