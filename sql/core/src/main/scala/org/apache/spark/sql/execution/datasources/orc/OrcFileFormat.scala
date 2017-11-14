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

package org.apache.spark.sql.execution.datasources.orc

import org.apache.orc.TypeDescription

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.StructType

private[sql] object OrcFileFormat {
  private def checkFieldName(name: String): Unit = {
    try {
      TypeDescription.fromString(s"struct<$name:int>")
    } catch {
      case _: IllegalArgumentException =>
        throw new AnalysisException(
          s"""Column name "$name" contains invalid character(s).
             |Please use alias to rename it.
           """.stripMargin.split("\n").mkString(" ").trim)
    }
  }

  def checkFieldNames(names: Seq[String]): Unit = {
    names.foreach(checkFieldName)
  }
}
