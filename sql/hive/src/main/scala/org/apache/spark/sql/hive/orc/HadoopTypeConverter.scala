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

package org.apache.spark.sql.hive.orc


import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive._

import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.hive.{HiveInspectors, HiveShim}

/**
 * We can consolidate TableReader.unwrappers and HiveInspectors.wrapperFor to use
 * this class.
 *
 */
private[hive] object HadoopTypeConverter extends HiveInspectors {
  /**
   * Builds specific unwrappers ahead of time according to object inspector
   * types to avoid pattern matching and branching costs per row.
   */
  def unwrappers(fieldRefs: Seq[StructField]): Seq[(Any, MutableRow, Int) => Unit] = fieldRefs.map {
    _.getFieldObjectInspector match {
      case oi: BooleanObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
      case oi: ByteObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
      case oi: ShortObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
      case oi: IntObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
      case oi: LongObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
      case oi: FloatObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
      case oi: DoubleObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
      case oi =>
        (value: Any, row: MutableRow, ordinal: Int) => row(ordinal) = unwrap(value, oi)
    }
  }

  /**
   * Wraps with Hive types based on object inspector.
   */
  def wrappers(oi: ObjectInspector): Any => Any = wrapperFor(oi)
}
