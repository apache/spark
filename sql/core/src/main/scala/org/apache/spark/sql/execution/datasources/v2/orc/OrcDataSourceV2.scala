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
package org.apache.spark.sql.execution.datasources.v2.orc

import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, Table}
import org.apache.spark.sql.types._

class OrcDataSourceV2 extends FileDataSourceV2 {

  override def fallBackFileFormat: Class[_ <: FileFormat] = classOf[OrcFileFormat]

  override def shortName(): String = "orc"

  private def getTableName(options: DataSourceOptions): String = {
    shortName() + ":" + options.paths().mkString(";")
  }

  override def getTable(options: DataSourceOptions): Table = {
    val tableName = getTableName(options)
    OrcTable(tableName, sparkSession, options, None)
  }

  override def getTable(options: DataSourceOptions, schema: StructType): Table = {
    val tableName = getTableName(options)
    OrcTable(tableName, sparkSession, options, Some(schema))
  }
}

object OrcDataSourceV2 {
  def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportsDataType(f.dataType) }

    case ArrayType(elementType, _) => supportsDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportsDataType(keyType) && supportsDataType(valueType)

    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _ => false
  }
}
