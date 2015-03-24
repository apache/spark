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

import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.common.`type`.HiveVarchar
import org.apache.hadoop.hive.common.`type`.{HiveDecimal, HiveVarchar}
import org.apache.spark.sql.hive.HiveShim
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.spark.sql.catalyst.expressions.{Row, MutableRow}

import scala.collection.JavaConversions._

/**
 * Wraps with Hive types based on object inspector.
 * TODO: Consolidate all hive OI/data interface code.
 */
private[hive] object HadoopTypeConverter extends HiveInspectors {
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
   * TODO: Consolidate all hive OI/data interface code.
   */

  def wrappers(oi: ObjectInspector): Any => Any = oi match {
    case _: JavaHiveVarcharObjectInspector =>
      (o: Any) => new HiveVarchar(o.asInstanceOf[String], o.asInstanceOf[String].size)

    case _: JavaHiveDecimalObjectInspector =>
      (o: Any) => HiveShim.createDecimal(o.asInstanceOf[BigDecimal].underlying())

    case soi: StandardStructObjectInspector =>
      val wrappers = soi.getAllStructFieldRefs.map(ref => wrapperFor(ref.getFieldObjectInspector))
      (o: Any) => {
        val struct = soi.create()
        (soi.getAllStructFieldRefs, wrappers, o.asInstanceOf[Row].toSeq).zipped.foreach {
          (field, wrapper, data) => soi.setStructFieldData(struct, field, wrapper(data))
        }
        struct
      }

    case loi: ListObjectInspector =>
      val wrapper = wrapperFor(loi.getListElementObjectInspector)
      (o: Any) => seqAsJavaList(o.asInstanceOf[Seq[_]].map(wrapper))

    case moi: MapObjectInspector =>
      val keyWrapper = wrapperFor(moi.getMapKeyObjectInspector)
      val valueWrapper = wrapperFor(moi.getMapValueObjectInspector)
      (o: Any) => mapAsJavaMap(o.asInstanceOf[Map[_, _]].map { case (key, value) =>
        keyWrapper(key) -> valueWrapper(value)
      })

    case _ =>
      identity[Any]
  }
}
