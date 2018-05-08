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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.types._


object ParquetUtils {

  /**
   * Verify if the schema is supported in Parquet datasource.
   */
  def verifySchema(schema: StructType): Unit = {
    def verifyType(dataType: DataType): Unit = dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
           StringType | BinaryType | DateType | TimestampType | _: DecimalType =>

      case st: StructType => st.foreach { f => verifyType(f.dataType) }

      case ArrayType(elementType, _) => verifyType(elementType)

      case MapType(keyType, valueType, _) =>
        verifyType(keyType)
        verifyType(valueType)

      case udt: UserDefinedType[_] => verifyType(udt.sqlType)

      case _ =>
        throw new UnsupportedOperationException(
          s"Parquet data source does not support ${dataType.simpleString} data type.")
    }

    schema.foreach(field => verifyType(field.dataType))
  }
}
