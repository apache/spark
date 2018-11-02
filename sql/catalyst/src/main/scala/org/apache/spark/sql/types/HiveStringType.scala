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

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.unsafe.types.UTF8String

/**
 * A hive string type for compatibility. These datatypes should only used for parsing,
 * and should NOT be used anywhere else. Any instance of these data types should be
 * replaced by a [[StringType]] before analysis.
 */
sealed abstract class HiveStringType extends AtomicType {
  private[sql] type InternalType = UTF8String

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  @transient private[sql] lazy val tag = typeTag[InternalType]

  override def defaultSize: Int = length

  private[spark] override def asNullable: HiveStringType = this

  def length: Int
}

object HiveStringType {
  def replaceCharType(dt: DataType): DataType = dt match {
    case ArrayType(et, nullable) =>
      ArrayType(replaceCharType(et), nullable)
    case MapType(kt, vt, nullable) =>
      MapType(replaceCharType(kt), replaceCharType(vt), nullable)
    case StructType(fields) =>
      StructType(fields.map { field =>
        field.copy(dataType = replaceCharType(field.dataType))
      })
    case _: HiveStringType => StringType
    case _ => dt
  }
}

/**
 * Hive char type. Similar to other HiveStringType's, these datatypes should only used for
 * parsing, and should NOT be used anywhere else. Any instance of these data types should be
 * replaced by a [[StringType]] before analysis.
 */
case class CharType(length: Int) extends HiveStringType {
  override def simpleString: String = s"char($length)"
}

/**
 * Hive varchar type. Similar to other HiveStringType's, these datatypes should only used for
 * parsing, and should NOT be used anywhere else. Any instance of these data types should be
 * replaced by a [[StringType]] before analysis.
 */
case class VarcharType(length: Int) extends HiveStringType {
  override def simpleString: String = s"varchar($length)"
}
