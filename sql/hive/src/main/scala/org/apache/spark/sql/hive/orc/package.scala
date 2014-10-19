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

import org.apache.spark.sql.{SQLContext, SchemaRDD}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{JavaHiveDecimalObjectInspector, JavaHiveVarcharObjectInspector}
import org.apache.hadoop.hive.common.`type`.{HiveDecimal, HiveVarchar}
import org.apache.spark.sql.catalyst.expressions.Row
/* Implicit conversions */
import scala.collection.JavaConversions._
package object orc {
  class OrcSchemaRDD(
       @transient val sqlContext1: SQLContext,
       @transient val baseLogicalPlan1: LogicalPlan)
    extends SchemaRDD(sqlContext1, baseLogicalPlan1) {
    /**
     * Saves the contents of this `SchemaRDD` as a ORC file, preserving the schema.  Files that
     * are written out using this method can be read back in as a SchemaRDD using the `orcFile`
     * function.
     * Note: you can only use it in HiveContext
     *
     * @group schema
     */
    def saveAsOrcFile(path: String): Unit = {
      sqlContext.executePlan(WriteToOrcFile(path, logicalPlan)).toRdd
    }
  }

  // TypeConverter for InsertIntoOrcTable
  object HadoopTypeConverter extends HiveInspectors {
    def wrap(a: (Any, ObjectInspector)): Any = a match {
      case (s: String, oi: JavaHiveVarcharObjectInspector) =>
        new HiveVarchar(s, s.size)

      case (bd: BigDecimal, oi: JavaHiveDecimalObjectInspector) =>
        new HiveDecimal(bd.underlying())

      case (row: Row, oi: StandardStructObjectInspector) =>
        val struct = oi.create()
        row.zip(oi.getAllStructFieldRefs: Seq[StructField]).foreach {
          case (data, field) =>
            oi.setStructFieldData(struct, field, wrap(data, field.getFieldObjectInspector))
        }
        struct
      case (s: Seq[_], oi: ListObjectInspector) =>
        val wrappedSeq = s.map(wrap(_, oi.getListElementObjectInspector))
        seqAsJavaList(wrappedSeq)

      case (m: Map[_, _], oi: MapObjectInspector) =>
        val keyOi = oi.getMapKeyObjectInspector
        val valueOi = oi.getMapValueObjectInspector
        val wrappedMap = m.map { case (key, value) => wrap(key, keyOi) -> wrap(value, valueOi) }
        mapAsJavaMap(wrappedMap)

      case (obj, _) =>
        obj
    }
  }

  // for orc compression type, only take effect in hive 0.13.1
  val orcDefaultCompressVar = "hive.exec.orc.default.compress"
  // for prediction push down in hive-0.13.1, don't enable it
  val ORC_FILTER_PUSHDOWN_ENABLED = false
  val SARG_PUSHDOWN = "sarg.pushdown"
}
