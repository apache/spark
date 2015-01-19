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

import java.io.{InputStream, OutputStream}
import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConversions._
import scala.language.implicitConversions

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.exec.{UDF, Utilities}
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table}
import org.apache.hadoop.hive.ql.plan.CreateTableDesc
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{HiveDecimalObjectInspector, PrimitiveObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.typeinfo.{DecimalTypeInfo, TypeInfo, TypeInfoFactory}
import org.apache.hadoop.hive.serde2.{io => hiveIo}

import org.apache.spark.sql.types.{Decimal, DecimalType}
import org.apache.spark.util.Utils._


/**
 * This class provides the UDF creation and also the UDF instance serialization and
 * de-serialization cross process boundary.
 *
 * Detail discussion can be found at https://github.com/apache/spark/pull/3640
 *
 * @param functionClassName UDF class name
 */
case class HiveFunctionWrapper(var functionClassName: String) extends java.io.Externalizable {
  // for Serialization
  def this() = this(null)

  @transient
  private val methodDeSerialize = {
    val method = classOf[Utilities].getDeclaredMethod(
      "deserializeObjectByKryo",
      classOf[Kryo],
      classOf[InputStream],
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
      classOf[OutputStream])
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
object HiveShim {
  val version = "0.13.1"

  def getDecimalWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.decimalTypeInfo, getDecimalWritable(value))

  def getDecimalWritable(value: Any): hiveIo.HiveDecimalWritable =
    if (value == null) {
      null
    } else {
      // TODO precise, scale?
      new hiveIo.HiveDecimalWritable(
        HiveUtils.newHiveDecimal(value.asInstanceOf[Decimal].toJavaBigDecimal))
    }

  def createDriverResultsArray = new JArrayList[Object]

  def processResults(results: JArrayList[Object]) = {
    results.map {
      case s: String => s
      case a: Array[Object] => a(0).asInstanceOf[String]
    }
  }

  def getExternalTmpPath(context: Context, path: Path) = {
    context.getExternalTmpPath(path.toUri)
  }

  def getAllPartitionsOf(client: Hive, tbl: Table) =  client.getAllPartitionsOf(tbl)

  def compatibilityBlackList = Seq()

  def setLocation(tbl: Table, crtTbl: CreateTableDesc): Unit = {
    tbl.setDataLocation(new Path(crtTbl.getLocation))
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
      Decimal(hdoi.getPrimitiveWritableObject(data).getHiveDecimal.bigDecimalValue,
        hdoi.precision(), hdoi.scale())
    } else {
      Decimal(hdoi.getPrimitiveJavaObject(data).bigDecimalValue(), hdoi.precision(), hdoi.scale())
    }
  }
}
