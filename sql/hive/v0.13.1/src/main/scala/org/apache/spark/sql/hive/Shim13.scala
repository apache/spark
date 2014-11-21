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

import java.util.{ArrayList => JArrayList}
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.hadoop.hive.common.`type`.{HiveDecimal}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.metadata.{Table, Hive, Partition}
import org.apache.hadoop.hive.ql.plan.{CreateTableDesc, FileSinkDesc, TableDesc}
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfo, DecimalTypeInfo, TypeInfoFactory}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{HiveDecimalObjectInspector, PrimitiveObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.{PrimitiveObjectInspector, ObjectInspector}
import org.apache.hadoop.hive.serde2.{Deserializer, ColumnProjectionUtils}
import org.apache.hadoop.hive.serde2.{io => hiveIo}
import org.apache.hadoop.{io => hadoopIo}
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.types.DecimalType
import org.apache.spark.sql.catalyst.types.decimal.Decimal

import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * A compatibility layer for interacting with Hive version 0.13.1.
 */
private[hive] object HiveShim {
  val version = "0.13.1"

  def getTableDesc(
    serdeClass: Class[_ <: Deserializer],
    inputFormatClass: Class[_ <: InputFormat[_, _]],
    outputFormatClass: Class[_],
    properties: Properties) = {
    new TableDesc(inputFormatClass, outputFormatClass, properties)
  }

  def getStringWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.stringTypeInfo,
      if (value == null) null else new hadoopIo.Text(value.asInstanceOf[String]))

  def getIntWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.intTypeInfo,
      if (value == null) null else new hadoopIo.IntWritable(value.asInstanceOf[Int]))

  def getDoubleWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.doubleTypeInfo, if (value == null) {
        null
      } else {
        new hiveIo.DoubleWritable(value.asInstanceOf[Double])
      })

  def getBooleanWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.booleanTypeInfo, if (value == null) {
        null
      } else {
        new hadoopIo.BooleanWritable(value.asInstanceOf[Boolean])
      })

  def getLongWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.longTypeInfo,
      if (value == null) null else new hadoopIo.LongWritable(value.asInstanceOf[Long]))

  def getFloatWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.floatTypeInfo, if (value == null) {
        null
      } else {
        new hadoopIo.FloatWritable(value.asInstanceOf[Float])
      })

  def getShortWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.shortTypeInfo,
      if (value == null) null else new hiveIo.ShortWritable(value.asInstanceOf[Short]))

  def getByteWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.byteTypeInfo,
      if (value == null) null else new hiveIo.ByteWritable(value.asInstanceOf[Byte]))

  def getBinaryWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.binaryTypeInfo, if (value == null) {
        null
      } else {
        new hadoopIo.BytesWritable(value.asInstanceOf[Array[Byte]])
      })

  def getDateWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.dateTypeInfo,
      if (value == null) null else new hiveIo.DateWritable(value.asInstanceOf[java.sql.Date]))

  def getTimestampWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.timestampTypeInfo, if (value == null) {
        null
      } else {
        new hiveIo.TimestampWritable(value.asInstanceOf[java.sql.Timestamp])
      })

  def getDecimalWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.decimalTypeInfo,
      if (value == null) {
        null
      } else {
        // TODO precise, scale?
        new hiveIo.HiveDecimalWritable(
          HiveShim.createDecimal(value.asInstanceOf[Decimal].toBigDecimal.underlying()))
      })

  def getPrimitiveNullWritableConstantObjectInspector: ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.voidTypeInfo, null)

  def createDriverResultsArray = new JArrayList[Object]

  def processResults(results: JArrayList[Object]) = {
    results.map { r =>
      r match {
        case s: String => s
        case a: Array[Object] => a(0).asInstanceOf[String]
      }
    }
  }

  def getStatsSetupConstTotalSize = StatsSetupConst.TOTAL_SIZE

  def getStatsSetupConstRawDataSize = StatsSetupConst.RAW_DATA_SIZE

  def createDefaultDBIfNeeded(context: HiveContext) = {
    context.runSqlHive("CREATE DATABASE default")
    context.runSqlHive("USE default")
  }

  def getCommandProcessor(cmd: Array[String], conf: HiveConf) = {
    CommandProcessorFactory.get(cmd, conf)
  }

  def createDecimal(bd: java.math.BigDecimal): HiveDecimal = {
    HiveDecimal.create(bd)
  }

  /*
   * This function in hive-0.13 become private, but we have to do this to walkaround hive bug
   */
  private def appendReadColumnNames(conf: Configuration, cols: Seq[String]) {
    val old: String = conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "")
    val result: StringBuilder = new StringBuilder(old)
    var first: Boolean = old.isEmpty

    for (col <- cols) {
      if (first) {
        first = false
      } else {
        result.append(',')
      }
      result.append(col)
    }
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, result.toString)
  }

  /*
   * Cannot use ColumnProjectionUtils.appendReadColumns directly, if ids is null or empty
   */
  def appendReadColumns(conf: Configuration, ids: Seq[Integer], names: Seq[String]) {
    if (ids != null && ids.size > 0) {
      ColumnProjectionUtils.appendReadColumns(conf, ids)
    }
    if (names != null && names.size > 0) {
      appendReadColumnNames(conf, names)
    }
  }

  def getExternalTmpPath(context: Context, path: Path) = {
    context.getExternalTmpPath(path.toUri)
  }

  def getDataLocationPath(p: Partition) = p.getDataLocation

  def getAllPartitionsOf(client: Hive, tbl: Table) =  client.getAllPartitionsOf(tbl)

  def compatibilityBlackList = Seq()

  def setLocation(tbl: Table, crtTbl: CreateTableDesc): Unit = {
    tbl.setDataLocation(new Path(crtTbl.getLocation()))
  }

  /*
   * Bug introdiced in hive-0.13. FileSinkDesc is serializable, but its member path is not.
   * Fix it through wrapper.
   * */
  implicit def wrapperToFileSinkDesc(w: ShimFileSinkDesc): FileSinkDesc = {
    var f = new FileSinkDesc(new Path(w.dir), w.tableInfo, w.compressed)
    f.setCompressCodec(w.compressCodec)
    f.setCompressType(w.compressType)
    f.setTableInfo(w.tableInfo)
    f.setDestTableId(w.destTableId)
    f
  }

  // Precision and scale to pass for unlimited decimals; these are the same as the precision and
  // scale Hive 0.13 infers for BigDecimals from sources that don't specify them (e.g. UDFs)
  private val UNLIMITED_DECIMAL_PRECISION = 38
  private val UNLIMITED_DECIMAL_SCALE = 18

  def decimalMetastoreString(decimalType: DecimalType): String = decimalType match {
    case DecimalType.Fixed(precision, scale) => s"decimal($precision,$scale)"
    case _ => s"decimal($UNLIMITED_DECIMAL_PRECISION,$UNLIMITED_DECIMAL_SCALE)"
  }

  def decimalTypeInfo(decimalType: DecimalType): TypeInfo = decimalType match {
    case DecimalType.Fixed(precision, scale) => new DecimalTypeInfo(precision, scale)
    case _ => new DecimalTypeInfo(UNLIMITED_DECIMAL_PRECISION, UNLIMITED_DECIMAL_SCALE)
  }

  def decimalTypeInfoToCatalyst(inspector: PrimitiveObjectInspector): DecimalType = {
    val info = inspector.getTypeInfo.asInstanceOf[DecimalTypeInfo]
    DecimalType(info.precision(), info.scale())
  }

  def toCatalystDecimal(hdoi: HiveDecimalObjectInspector, data: Any): Decimal = {
    Decimal(hdoi.getPrimitiveJavaObject(data).bigDecimalValue(), hdoi.precision(), hdoi.scale())
  }
}

/*
 * Bug introdiced in hive-0.13. FileSinkDesc is serilizable, but its member path is not.
 * Fix it through wrapper.
 */
class ShimFileSinkDesc(var dir: String, var tableInfo: TableDesc, var compressed: Boolean)
  extends Serializable with Logging {
  var compressCodec: String = _
  var compressType: String = _
  var destTableId: Int = _

  def setCompressed(compressed: Boolean) {
    this.compressed = compressed
  }

  def getDirName = dir

  def setDestTableId(destTableId: Int) {
    this.destTableId = destTableId
  }

  def setTableInfo(tableInfo: TableDesc) {
    this.tableInfo = tableInfo
  }

  def setCompressCodec(intermediateCompressorCodec: String) {
    compressCodec = intermediateCompressorCodec
  }

  def setCompressType(intermediateCompressType: String) {
    compressType = intermediateCompressType
  }
}
