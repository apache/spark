/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hive

import java.math.{BigDecimal => JBigDecimal}
import java.util.{Properties, Set => JSet}

import scala.collection.JavaConversions._
import scala.language.{existentials, implicitConversions}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table}
import org.apache.hadoop.hive.ql.plan.{FileSinkDesc, TableDesc}
import org.apache.hadoop.hive.ql.processors.{CommandProcessor, CommandProcessorFactory}
import org.apache.hadoop.hive.serde2.{ColumnProjectionUtils, Deserializer}
import org.apache.hadoop.mapred.InputFormat

import org.apache.spark.Logging

/**
 * A utility object used to cope with Hive compatibility issues.
 */
object HiveCompat {
  def createDefaultDBIfNeeded(context: HiveContext) = {
    context.runSqlHive("CREATE DATABASE IF NOT EXISTS default")
    context.runSqlHive("USE default")
  }

  def newTableDesc(
      serdeClass: Class[_ <: Deserializer],
      inputFormatClass: Class[_ <: InputFormat[_, _]],
      outputFormatClass: Class[_],
      properties: Properties) = callWithAlternatives(
    // For Hive 0.13.1
    Construct(classOf[TableDesc],
      classOf[Class[_ <: InputFormat[_, _]]] -> inputFormatClass,
      classOf[Class[_]] -> outputFormatClass,
      classOf[Properties] -> properties),

    // For Hive 0.12.0
    Construct(classOf[TableDesc],
      classOf[Class[_ <: Deserializer]] -> serdeClass,
      classOf[Class[_ <: InputFormat[_, _]]] -> inputFormatClass,
      classOf[Class[_]] -> outputFormatClass,
      classOf[Properties] -> properties))

  def getCommandProcessor(cmd: Array[String], conf: HiveConf) = {
    callWithAlternatives[CommandProcessor](
      // For Hive 0.13.1
      InvokeStatic[CommandProcessor](classOf[CommandProcessorFactory], "get",
        classOf[Array[String]] -> cmd,
        classOf[HiveConf] -> conf),

      // For Hive 0.12.0
      InvokeStatic[CommandProcessor](classOf[CommandProcessorFactory], "get",
        classOf[String] -> cmd(0),
        classOf[HiveConf] -> conf))
  }

  def getAllPartitionsOf(client: Hive, tbl: Table) = callWithAlternatives[JSet[Partition]](
    // For Hive 0.13.1
    Invoke[JSet[Partition]](classOf[Hive], client, "getAllPartitionsOf",
      classOf[Table] -> tbl),

    // For Hive 0.12.0
    Invoke[JSet[Partition]](classOf[Hive], client, "getAllPartitionsForPruner",
      classOf[Table] -> tbl)
  )

  def newHiveDecimal(bd: JBigDecimal) = callWithAlternatives(
    // For Hive 0.13.1
    InvokeStatic[HiveDecimal](classOf[HiveDecimal], "create",
      classOf[JBigDecimal] -> bd),

    // For Hive 0.12.0
    Construct(classOf[HiveDecimal],
      classOf[JBigDecimal] -> bd))

  // Hive 0.13.1: org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE
  // Hive 0.12.0: org.apache.hadoop.hive.ql.stats.StatsSetupConst.TOTAL_SIZE
  val getStatsSetupConstTotalSize = "totalSize"

  // Hive 0.13.1: org.apache.hadoop.hive.common.StatsSetupConst.RAW_DATA_SIZE
  // Hive 0.12.0: org.apache.hadoop.hive.ql.stats.StatsSetupConst.RAW_DATA_SIZE
  val getStatsSetupConstRawDataSize = "rawDataSize"

  def appendReadColumns(conf: Configuration, ids: Seq[Integer], names: Seq[String]) {
    // ColumnProjectUtils.appendReadColumnNames was made private in Hive 0.13.1, have to
    // re-implement it here to workaround Hive bugs.
    def appendReadColumnNames(cols: Seq[String]) {
      val original = Option(conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, null)).toSeq
      val merged = (original ++ names).mkString(",")
      conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, merged)
    }

    if (ids.nonEmpty) ColumnProjectionUtils.appendReadColumnIDs(conf, ids)
    if (names.nonEmpty) appendReadColumnNames(names)
  }

  def getDataLocationPath(p: Partition) = p.getPath.apply(0)

  /**
   * This class is used to workaround a bug introduced in Hive 0.13.1, which makes `FileSinkDesc` non-
   * serializable
   */
  class FileSinkDescWrapper(val dirName: String, var tableInfo: TableDesc, var compressed: Boolean)
    extends Serializable with Logging {

    var compressCodec: String = _
    var compressType: String = _

    // Used to workaround `FileSinkDesc` constructor incompatibility between Hive 0.12.0 and 0.13.1.
    // The type of the `dirName` argument in 0.12.0 is `String`, but `Path` in 0.13.1.
    private implicit def stringToPath(string: String): Path = new Path(dirName)

    def value = {
      val f = new FileSinkDesc(dirName, tableInfo, compressed)
      f.setCompressCodec(compressCodec)
      f.setCompressType(compressType)
      f.setTableInfo(tableInfo)
      f
    }
  }

  // TODO Move these reflection utilities to ReflectionUtils

  trait ReflectedCall[T] { def call: T }

  case class Construct[T](clazz: Class[T], args: (Class[_], AnyRef)*) extends ReflectedCall[T] {
    def call: T = {
      val (argTypes, argValues) = args.unzip
      val constructor = clazz.getConstructor(argTypes: _*)
      constructor.setAccessible(true)
      constructor.newInstance(argValues: _*)
    }
  }

  case class Invoke[U](clazz: Class[_], obj: AnyRef, methodName: String, args: (Class[_], AnyRef)*)
    extends ReflectedCall[U] {

    def call: U = {
      val (argTypes, argValues) = args.unzip
      val method = clazz.getMethod(methodName, argTypes: _*)
      method.setAccessible(true)
      method.invoke(obj, argValues: _*).asInstanceOf[U]
    }
  }

  case class InvokeStatic[U](clazz: Class[_], methodName: String, args: (Class[_], AnyRef)*)
    extends ReflectedCall[U] {

    def call: U = Invoke[U](clazz, null, methodName, args: _*).call
  }

  def callWithAlternatives[T](alternatives: ReflectedCall[T]*): T = {
    alternatives.foreach { alternative =>
      try return alternative.call catch { case e: NoSuchMethodException =>
        // Just ignore and fallback to the next alternative.
      }
    }

    throw new RuntimeException("No available alternative method/constructor can be invoked.")
  }
}
