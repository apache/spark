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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{Cast, EmptyRow, Literal}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.sql.util.SchemaUtils

object SparkResult {

  /**
   * Returns the result as Apache Spark sequence of strings.
   *
   * This is used in `SparkSQLDriver` for CLI applications.
   */
  def sparkResultString(queryExecution: QueryExecution): Seq[String] = {
    val types = queryExecution.executedPlan.output.map(_.dataType)
    val rows = queryExecution.executedPlan.executeCollect().map(_.toSeq(types)).toSeq

    // Reformat to match hive tab delimited output.
    rows.map(_.zip(types).map(e => toSparkString(e))).map(_.mkString("\t"))
  }

  def toSparkString(cell: (Any, DataType)): String = {
    val dataType = cell._2
    val value = cell._1
    // Since binary types in top-level schema fields have a specific format to print,
    // so we do not cast them to strings here.
    val castValue = if (dataType == BinaryType) {
      value
    } else {
      val zone: String = SQLConf.get.sessionLocalTimeZone
      val cast = Cast(Literal(value, dataType), StringType, Some(zone))
      cast.eval(EmptyRow)
    }

    // For array values, replace Seq and Array with square brackets
    castValue match {
      case null => "NULL"
      case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
      case other =>
        // Escapes meta-characters not to break the `showString` format
        SchemaUtils.escapeMetaCharacters(other.toString)
    }
  }
}
