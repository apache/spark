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

package org.apache.spark

/**
 * Allows the execution of relational queries, including those expressed in SQL using Spark.
 *
 * Note that this package is located in catalyst instead of in core so that all subprojects can
 * inherit the settings from this package object.
 */
package object sql {

  protected[sql] def Logger(name: String) =
    com.typesafe.scalalogging.slf4j.Logger(org.slf4j.LoggerFactory.getLogger(name))

  protected[sql] type Logging = com.typesafe.scalalogging.slf4j.Logging

  type Row = catalyst.expressions.Row

  val Row = catalyst.expressions.Row

  type DataType = catalyst.types.DataType

  val DataType = catalyst.types.DataType

  val NullType = catalyst.types.NullType

  val StringType = catalyst.types.StringType

  val BinaryType = catalyst.types.BinaryType

  val BooleanType = catalyst.types.BooleanType

  val TimestampType = catalyst.types.TimestampType

  val DecimalType = catalyst.types.DecimalType

  val DoubleType = catalyst.types.DoubleType

  val FloatType = catalyst.types.FloatType

  val ByteType = catalyst.types.ByteType

  val IntegerType = catalyst.types.IntegerType

  val LongType = catalyst.types.LongType

  val ShortType = catalyst.types.ShortType

  type ArrayType = catalyst.types.ArrayType

  val ArrayType = catalyst.types.ArrayType

  type MapType = catalyst.types.MapType

  val MapType = catalyst.types.MapType

  type StructType = catalyst.types.StructType

  val StructType = catalyst.types.StructType

  type StructField = catalyst.types.StructField

  val StructField = catalyst.types.StructField
}
