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

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import com.google.common.base.Objects
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.SerializationUtilities
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.hive.ql.plan.{FileSinkDesc, TableDesc}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMacro
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils
import org.apache.hadoop.hive.serde2.avro.{AvroGenericRecordWritable, AvroSerdeUtils}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector
import org.apache.hadoop.io.Writable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.Decimal
import org.apache.spark.util.Utils

private[hive] object HiveShim {
  // Precision and scale to pass for unlimited decimals; these are the same as the precision and
  // scale Hive 0.13 infers for BigDecimals from sources that don't specify them (e.g. UDFs)
  val UNLIMITED_DECIMAL_PRECISION = 38
  val UNLIMITED_DECIMAL_SCALE = 18
  val HIVE_GENERIC_UDF_MACRO_CLS = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFMacro"

  /*
   * This function in hive-0.13 become private, but we have to do this to work around hive bug
   */
  private def appendReadColumnNames(conf: Configuration, cols: Seq[String]): Unit = {
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
   * Cannot use ColumnProjectionUtils.appendReadColumns directly, if ids is null
   */
  def appendReadColumns(conf: Configuration, ids: Seq[Integer], names: Seq[String]): Unit = {
    if (ids != null) {
      ColumnProjectionUtils.appendReadColumns(conf, ids.asJava)
    }
    if (names != null) {
      appendReadColumnNames(conf, names)
    }
  }

  /*
   * Bug introduced in hive-0.13. AvroGenericRecordWritable has a member recordReaderID that
   * is needed to initialize before serialization.
   */
  def prepareWritable(w: Writable, serDeProps: Seq[(String, String)]): Writable = {
    w match {
      case w: AvroGenericRecordWritable =>
        w.setRecordReaderID(new UID())
        // In Hive 1.1, the record's schema may need to be initialized manually or a NPE will
        // be thrown.
        if (w.getFileSchema() == null) {
          serDeProps
            .find(_._1 == AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName())
            .foreach { kv =>
              w.setFileSchema(new Schema.Parser().parse(kv._2))
            }
        }
      case _ =>
    }
    w
  }

  def toCatalystDecimal(hdoi: HiveDecimalObjectInspector, data: Any): Decimal = {
    if (hdoi.preferWritable()) {
      val value = hdoi.getPrimitiveWritableObject(data)
      if (value == null) {
        null
      } else {
        Decimal(value.getHiveDecimal().bigDecimalValue, hdoi.precision(), hdoi.scale())
      }
    } else {
      val value = hdoi.getPrimitiveJavaObject(data)
      if (value == null) {
        null
      } else {
        Decimal(value.bigDecimalValue(), hdoi.precision(), hdoi.scale())
      }
    }
  }

  /**
   * This class provides the UDF creation and also the UDF instance serialization and
   * de-serialization cross process boundary.
   *
   * Detail discussion can be found at https://github.com/apache/spark/pull/3640
   *
   * @param functionClassName UDF class name
   * @param instance optional UDF instance which contains additional information (for macro)
   * @param clazz optional class instance to create UDF instance
   */
  private[hive] case class HiveFunctionWrapper(
      var functionClassName: String,
      private var instance: AnyRef = null,
      private var clazz: Class[_ <: AnyRef] = null) extends java.io.Externalizable {

    // for Serialization
    def this() = this(null)

    override def hashCode(): Int = {
      if (functionClassName == HIVE_GENERIC_UDF_MACRO_CLS) {
        Objects.hashCode(functionClassName, instance.asInstanceOf[GenericUDFMacro].getBody())
      } else {
        functionClassName.hashCode()
      }
    }

    override def equals(other: Any): Boolean = other match {
      case a: HiveFunctionWrapper if functionClassName == a.functionClassName =>
        // In case of udf macro, check to make sure they point to the same underlying UDF
        if (functionClassName == HIVE_GENERIC_UDF_MACRO_CLS) {
          a.instance.asInstanceOf[GenericUDFMacro].getBody() ==
            instance.asInstanceOf[GenericUDFMacro].getBody()
        } else {
          true
        }
      case _ => false
    }

    def deserializePlan[UDFType](is: java.io.InputStream, clazz: Class[_]): UDFType = {
      SerializationUtilities.deserializePlan(is, clazz).asInstanceOf[UDFType]
    }

    def serializePlan(function: AnyRef, out: java.io.OutputStream): Unit = {
      SerializationUtilities.serializePlan(function, out)
    }

    def writeExternal(out: java.io.ObjectOutput): Unit = {
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

    def readExternal(in: java.io.ObjectInput): Unit = {
      // read the function name
      functionClassName = in.readUTF()

      if (in.readBoolean()) {
        // if the instance is not null
        // read the function in bytes
        val functionInBytesLength = in.readInt()
        val functionInBytes = new Array[Byte](functionInBytesLength)
        in.readFully(functionInBytes)

        // deserialize the function object via Hive Utilities
        clazz = Utils.getContextOrSparkClassLoader.loadClass(functionClassName)
          .asInstanceOf[Class[_ <: AnyRef]]
        instance = deserializePlan[AnyRef](new java.io.ByteArrayInputStream(functionInBytes),
          clazz)
      }
    }

    def createFunction[UDFType <: AnyRef](): UDFType = {
      if (instance != null) {
        instance.asInstanceOf[UDFType]
      } else {
        if (clazz == null) {
          clazz = Utils.getContextOrSparkClassLoader.loadClass(functionClassName)
            .asInstanceOf[Class[_ <: AnyRef]]
        }
        val func = clazz.getConstructor().newInstance().asInstanceOf[UDFType]
        if (!func.isInstanceOf[UDF]) {
          // We cache the function if it's no the Simple UDF,
          // as we always have to create new instance for Simple UDF
          instance = func
        }
        func
      }
    }
  }

  /*
   * Bug introduced in hive-0.13. FileSinkDesc is serializable, but its member path is not.
   * Fix it through wrapper.
   */
  implicit def wrapperToFileSinkDesc(w: ShimFileSinkDesc): FileSinkDesc = {
    val f = new FileSinkDesc(new Path(w.dir), w.tableInfo, w.compressed)
    f.setCompressCodec(w.compressCodec)
    f.setCompressType(w.compressType)
    f.setTableInfo(w.tableInfo)
    f.setDestTableId(w.destTableId)
    f
  }

  /*
   * Bug introduced in hive-0.13. FileSinkDesc is serializable, but its member path is not.
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

    def setCompressed(compressed: Boolean): Unit = {
      this.compressed = compressed
    }

    def getDirName(): String = dir

    def setDestTableId(destTableId: Int): Unit = {
      this.destTableId = destTableId
    }

    def setTableInfo(tableInfo: TableDesc): Unit = {
      this.tableInfo = tableInfo
    }

    def setCompressCodec(intermediateCompressorCodec: String): Unit = {
      compressCodec = intermediateCompressorCodec
    }

    def setCompressType(intermediateCompressType: String): Unit = {
      compressType = intermediateCompressType
    }
  }
}
