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

import java.rmi.server.UID
import java.util.{Properties, ArrayList => JArrayList}

import scala.collection.JavaConversions._
import scala.language.implicitConversions

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.exec.{UDF, Utilities}
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table}
import org.apache.hadoop.hive.ql.plan.{CreateTableDesc, FileSinkDesc, TableDesc}
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{HiveDecimalObjectInspector, PrimitiveObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorConverters, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.typeinfo.{DecimalTypeInfo, TypeInfo, TypeInfoFactory}
import org.apache.hadoop.hive.serde2.{ColumnProjectionUtils, Deserializer, io => hiveIo}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.{io => hadoopIo}

import org.apache.spark.Logging
import org.apache.spark.sql.types.{Decimal, DecimalType, UTF8String}

/**
 * This class provides the UDF creation and also the UDF instance serialization and
 * de-serialization cross process boundary.
 * 
 * Detail discussion can be found at https://github.com/apache/spark/pull/3640
 *
 * @param functionClassName UDF class name
 */
private[hive] case class HiveFunctionWrapper(var functionClassName: String)
  extends java.io.Externalizable {

  // for Serialization
  def this() = this(null)

  import org.apache.spark.util.Utils._

  @transient
  private val methodDeSerialize = {
    val method = classOf[Utilities].getDeclaredMethod(
      "deserializeObjectByKryo",
      classOf[Kryo],
      classOf[java.io.InputStream],
      classOf[Class[_]])
    method.setAccessible(true)

    method
  }

  @transient
  private val methodSerialize = {
    val method = classOf[Utilities].getDeclaredMethod(
      "serializeObjectByKryo",
      classOf[Kryo],
      classOf[Object],
      classOf[java.io.OutputStream])
    method.setAccessible(true)

    method
  }

  def deserializePlan[UDFType](is: java.io.InputStream, clazz: Class[_]): UDFType = {
    methodDeSerialize.invoke(null, Utilities.runtimeSerializationKryo.get(), is, clazz)
      .asInstanceOf[UDFType]
  }

  def serializePlan(function: AnyRef, out: java.io.OutputStream): Unit = {
    methodSerialize.invoke(null, Utilities.runtimeSerializationKryo.get(), function, out)
  }

  private var instance: AnyRef = null

  def writeExternal(out: java.io.ObjectOutput) {
    // output the function name
    out.writeUTF(functionClassName)

    // Write a flag if instance is null or not
    out.writeBoolean(instance != null)
    if (instance != null) {
      // Some of the UDF are serializable, but some others are not
      // Hive Utilities can handle both cases
      val baos = new java.io.ByteArrayOutputStream()
      serializePlan(instance, baos)
      val functionInBytes = baos.toByteArray

      // output the function bytes
      out.writeInt(functionInBytes.length)
      out.write(functionInBytes, 0, functionInBytes.length)
    }
  }

  def readExternal(in: java.io.ObjectInput) {
    // read the function name
    functionClassName = in.readUTF()

    if (in.readBoolean()) {
      // if the instance is not null
      // read the function in bytes
      val functionInBytesLength = in.readInt()
      val functionInBytes = new Array[Byte](functionInBytesLength)
      in.read(functionInBytes, 0, functionInBytesLength)

      // deserialize the function object via Hive Utilities
      instance = deserializePlan[AnyRef](new java.io.ByteArrayInputStream(functionInBytes),
        getContextOrSparkClassLoader.loadClass(functionClassName))
    }
  }

  def createFunction[UDFType <: AnyRef](): UDFType = {
    if (instance != null) {
      instance.asInstanceOf[UDFType]
    } else {
      val func = getContextOrSparkClassLoader
                   .loadClass(functionClassName).newInstance.asInstanceOf[UDFType]
      if (!func.isInstanceOf[UDF]) {
        // We cache the function if it's no the Simple UDF,
        // as we always have to create new instance for Simple UDF
        instance = func
      }
      func
    }
  }
}

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
      TypeInfoFactory.stringTypeInfo, getStringWritable(value))

  def getIntWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.intTypeInfo, getIntWritable(value))

  def getDoubleWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.doubleTypeInfo, getDoubleWritable(value))

  def getBooleanWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.booleanTypeInfo, getBooleanWritable(value))

  def getLongWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.longTypeInfo, getLongWritable(value))

  def getFloatWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.floatTypeInfo, getFloatWritable(value))

  def getShortWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.shortTypeInfo, getShortWritable(value))

  def getByteWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.byteTypeInfo, getByteWritable(value))

  def getBinaryWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.binaryTypeInfo, getBinaryWritable(value))

  def getDateWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.dateTypeInfo, getDateWritable(value))

  def getTimestampWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.timestampTypeInfo, getTimestampWritable(value))

  def getDecimalWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.decimalTypeInfo, getDecimalWritable(value))

  def getPrimitiveNullWritableConstantObjectInspector: ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.voidTypeInfo, null)

  def getStringWritable(value: Any): hadoopIo.Text =
    if (value == null) null else new hadoopIo.Text(value.asInstanceOf[UTF8String].toString)

  def getIntWritable(value: Any): hadoopIo.IntWritable =
    if (value == null) null else new hadoopIo.IntWritable(value.asInstanceOf[Int])

  def getDoubleWritable(value: Any): hiveIo.DoubleWritable =
    if (value == null) {
      null
    } else {
      new hiveIo.DoubleWritable(value.asInstanceOf[Double])
    }

  def getBooleanWritable(value: Any): hadoopIo.BooleanWritable =
    if (value == null) {
      null
    } else {
      new hadoopIo.BooleanWritable(value.asInstanceOf[Boolean])
    }

  def getLongWritable(value: Any): hadoopIo.LongWritable =
    if (value == null) null else new hadoopIo.LongWritable(value.asInstanceOf[Long])

  def getFloatWritable(value: Any): hadoopIo.FloatWritable =
    if (value == null) {
      null
    } else {
      new hadoopIo.FloatWritable(value.asInstanceOf[Float])
    }

  def getShortWritable(value: Any): hiveIo.ShortWritable =
    if (value == null) null else new hiveIo.ShortWritable(value.asInstanceOf[Short])

  def getByteWritable(value: Any): hiveIo.ByteWritable =
    if (value == null) null else new hiveIo.ByteWritable(value.asInstanceOf[Byte])

  def getBinaryWritable(value: Any): hadoopIo.BytesWritable =
    if (value == null) {
      null
    } else {
      new hadoopIo.BytesWritable(value.asInstanceOf[Array[Byte]])
    }

  def getDateWritable(value: Any): hiveIo.DateWritable =
    if (value == null) null else new hiveIo.DateWritable(value.asInstanceOf[Int])

  def getTimestampWritable(value: Any): hiveIo.TimestampWritable =
    if (value == null) {
      null
    } else {
      new hiveIo.TimestampWritable(value.asInstanceOf[java.sql.Timestamp])
    }

  def getDecimalWritable(value: Any): hiveIo.HiveDecimalWritable =
    if (value == null) {
      null
    } else {
      // TODO precise, scale?
      new hiveIo.HiveDecimalWritable(
        HiveShim.createDecimal(value.asInstanceOf[Decimal].toJavaBigDecimal))
    }

  def getPrimitiveNullWritable: NullWritable = NullWritable.get()

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
    if (hdoi.preferWritable()) {
      Decimal(hdoi.getPrimitiveWritableObject(data).getHiveDecimal().bigDecimalValue,
        hdoi.precision(), hdoi.scale())
    } else {
      Decimal(hdoi.getPrimitiveJavaObject(data).bigDecimalValue(), hdoi.precision(), hdoi.scale())
    }
  }

  def getConvertedOI(inputOI: ObjectInspector, outputOI: ObjectInspector): ObjectInspector = {
    ObjectInspectorConverters.getConvertedOI(inputOI, outputOI)
  }

  /*
   * Bug introduced in hive-0.13. AvroGenericRecordWritable has a member recordReaderID that
   * is needed to initialize before serialization.
   */
  def prepareWritable(w: Writable): Writable = {
    w match {
      case w: AvroGenericRecordWritable =>
        w.setRecordReaderID(new UID())
      case _ =>
    }
    w
  }

  def setTblNullFormat(crtTbl: CreateTableDesc, tbl: Table) = {
    if (crtTbl != null && crtTbl.getNullFormat() != null) {
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_NULL_FORMAT, crtTbl.getNullFormat())
    }
  }
}

/*
 * Bug introduced in hive-0.13. FileSinkDesc is serilizable, but its member path is not.
 * Fix it through wrapper.
 */
private[hive] class ShimFileSinkDesc(
    var dir: String,
    var tableInfo: TableDesc,
    var compressed: Boolean)
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
