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

package org.apache.spark.sql.jdbc

import java.sql.ResultSet

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.{ExistingRdd, SparkLogicalPlan}
import org.apache.spark.Logging

private[sql] object JdbcResultSetRDD extends Logging {

  private[sql] def inferSchema(
      jdbcResultSet: JdbcRDD[ResultSet]): StructType = {
    StructType(createSchema(jdbcResultSet.getSchema))
  }

  private def createSchema(metaSchema: Seq[(String, Int, Boolean)]): Seq[StructField] = {
    metaSchema.map(e => StructField(e._1, JdbcTypes.toPrimitiveDataType(e._2), e._3))
  }

  private[sql] def jdbcResultSetToRow(
      jdbcResultSet: JdbcRDD[ResultSet],
      schema: StructType) : RDD[Row] = {
    val row = new GenericMutableRow(schema.fields.length)
    jdbcResultSet.map(asRow(_, row, schema.fields))
  }

  private def asRow(rs: ResultSet, row: GenericMutableRow, schemaFields: Seq[StructField]): Row = {
    var i = 0
    while (i < schemaFields.length) {
      schemaFields(i).dataType match {
        case StringType  => row.update(i, rs.getString(i + 1))
        case DecimalType => row.update(i, rs.getBigDecimal(i + 1))
        case BooleanType => row.update(i, rs.getBoolean(i + 1))
        case ByteType    => row.update(i, rs.getByte(i + 1))
        case ShortType   => row.update(i, rs.getShort(i + 1))
        case IntegerType => row.update(i, rs.getInt(i + 1))
        case LongType    => row.update(i, rs.getLong(i + 1))
        case FloatType   => row.update(i, rs.getFloat(i + 1))
        case DoubleType  => row.update(i, rs.getDouble(i + 1))
        case BinaryType  => row.update(i, rs.getBytes(i + 1))
        case TimestampType => row.update(i, rs.getTimestamp(i + 1))
        case _ => sys.error(
          s"Unsupported jdbc datatype")
      }
      if (rs.wasNull) row.update(i, null)
      i += 1
    }

    row
  }
}
