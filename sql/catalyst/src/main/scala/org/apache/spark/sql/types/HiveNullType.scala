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

package org.apache.spark.sql.types

/**
 * A hive null type for compatibility. These datatypes should only used for parsing,
 * and should NOT be used anywhere else. Any instance of these data types should be
 * replaced by a [[NullType]] before analysis.
 */
class HiveNullType private() extends DataType {

  override def defaultSize: Int = 1

  override private[spark] def asNullable: HiveNullType = this

  override def simpleString: String = "void"
}

case object HiveNullType extends HiveNullType {
  def replaceNullType(dt: DataType): DataType = dt match {
    case ArrayType(et, nullable) =>
      ArrayType(replaceNullType(et), nullable)
    case MapType(kt, vt, nullable) =>
      MapType(replaceNullType(kt), replaceNullType(vt), nullable)
    case StructType(fields) =>
      StructType(fields.map { field =>
        field.copy(dataType = replaceNullType(field.dataType))
      })
    case _: HiveNullType => NullType
    case _ => dt
  }


  def containsNullType(dt: DataType): Boolean = dt match {
    case ArrayType(et, _) => containsNullType(et)
    case MapType(kt, vt, _) => containsNullType(kt) || containsNullType(vt)
    case StructType(fields) => fields.exists(f => containsNullType(f.dataType))
    case _ => dt.isInstanceOf[HiveNullType]
  }
}
