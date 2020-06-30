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
 * A hive void type for compatibility. These datatypes should only used for parsing,
 * and should NOT be used anywhere else. Any instance of these data types should be
 * replaced by a [[NullType]] before analysis.
 */
class HiveVoidType private() extends DataType {

  override def defaultSize: Int = 1

  override private[spark] def asNullable: HiveVoidType = this

  override def simpleString: String = "void"
}

case object HiveVoidType extends HiveVoidType {
  def replaceVoidType(dt: DataType): DataType = dt match {
    case ArrayType(et, nullable) =>
      ArrayType(replaceVoidType(et), nullable)
    case MapType(kt, vt, nullable) =>
      MapType(replaceVoidType(kt), replaceVoidType(vt), nullable)
    case StructType(fields) =>
      StructType(fields.map(f => f.copy(dataType = replaceVoidType(f.dataType))))
    case _: HiveVoidType => NullType
    case _ => dt
  }

  def containsVoidType(dt: DataType): Boolean = dt match {
    case ArrayType(et, _) => containsVoidType(et)
    case MapType(kt, vt, _) => containsVoidType(kt) || containsVoidType(vt)
    case StructType(fields) => fields.exists(f => containsVoidType(f.dataType))
    case _ => dt.isInstanceOf[HiveVoidType]
  }
}
