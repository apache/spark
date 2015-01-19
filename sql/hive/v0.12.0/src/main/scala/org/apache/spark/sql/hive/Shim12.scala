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

import scala.language.implicitConversions

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table}
import org.apache.hadoop.hive.ql.plan.CreateTableDesc
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{HiveDecimalObjectInspector, PrimitiveObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfo, TypeInfoFactory}
import org.apache.hadoop.hive.serde2.{io => hiveIo}

import org.apache.spark.sql.types.{Decimal, DecimalType}

case class HiveFunctionWrapper(functionClassName: String) extends java.io.Serializable {
  // for Serialization
  def this() = this(null)

  import org.apache.spark.util.Utils._
  def createFunction[UDFType <: AnyRef](): UDFType = {
    getContextOrSparkClassLoader
      .loadClass(functionClassName).newInstance.asInstanceOf[UDFType]
  }
}

/**
 * A compatibility layer for interacting with Hive version 0.12.0.
 */
object HiveShim {
  val version = "0.12.0"

  def getDecimalWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory.DECIMAL,
      getDecimalWritable(value))

  def getDecimalWritable(value: Any): hiveIo.HiveDecimalWritable =
    if (value == null) {
      null
    } else {
      new hiveIo.HiveDecimalWritable(
        HiveUtils.newHiveDecimal(value.asInstanceOf[Decimal].toJavaBigDecimal))
    }

  def createDriverResultsArray = new JArrayList[String]

  def processResults(results: JArrayList[String]) = results

  def getExternalTmpPath(context: Context, uri: URI) = {
    context.getExternalTmpFileURI(uri)
  }

  def getAllPartitionsOf(client: Hive, tbl: Table) =  client.getAllPartitionsForPruner(tbl)

  def compatibilityBlackList = Seq(
    "decimal_.*",
    "udf7",
    "drop_partitions_filter2",
    "show_.*",
    "serde_regex",
    "udf_to_date",
    "udaf_collect_set",
    "udf_concat"
  )

  def setLocation(tbl: Table, crtTbl: CreateTableDesc): Unit = {
    tbl.setDataLocation(new Path(crtTbl.getLocation).toUri)
  }

  def decimalMetastoreString(decimalType: DecimalType): String = "decimal"

  def decimalTypeInfo(decimalType: DecimalType): TypeInfo =
    TypeInfoFactory.decimalTypeInfo

  def decimalTypeInfoToCatalyst(inspector: PrimitiveObjectInspector): DecimalType = {
    DecimalType.Unlimited
  }

  def toCatalystDecimal(hdoi: HiveDecimalObjectInspector, data: Any): Decimal = {
    if (hdoi.preferWritable()) {
      Decimal(hdoi.getPrimitiveWritableObject(data).getHiveDecimal.bigDecimalValue)
    } else {
      Decimal(hdoi.getPrimitiveJavaObject(data).bigDecimalValue())
    }
  }
}
