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

import org.apache.parquet.io.api.Converter

import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.types._

private[parquet] object ParquetUpCastConversion {

  // The logic for setting and adding a value in `ParquetPrimitiveConverter` are separated so that
  // the value can be converted to a desired type. `NumericValueUpdater` updates the input numbers
  // via `ParentContainerUpdater`.
  private type NumericValueUpdater = Number => Unit

  private def makeNumericConverter(
      guessedType: DataType,
      updater: ParentContainerUpdater): NumericValueUpdater => ParquetPrimitiveConverter = {
    guessedType match {
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
        (valueUpdater: NumericValueUpdater) =>
          new ParquetPrimitiveConverter(updater) {
            override def addInt(value: Int): Unit = valueUpdater(value)
            override def addLong(value: Long): Unit = valueUpdater(value)
            override def addFloat(value: Float): Unit = valueUpdater(value)
            override def addDouble(value: Double): Unit = valueUpdater(value)
          }
    }
  }

  private def makeNumericUpdater(
      catalystType: DataType,
      updater: ParentContainerUpdater): NumericValueUpdater = catalystType match {
    case ByteType => (v: Number) => updater.setByte(v.byteValue())
    case ShortType => (v: Number) => updater.setShort(v.shortValue())
    case IntegerType => (v: Number) => updater.setInt(v.intValue())
    case LongType => (v: Number) => updater.setLong(v.longValue())
    case FloatType => (v: Number) => updater.setFloat(v.floatValue())
    case DoubleType => (v: Number) => updater.setDouble(v.doubleValue())
  }

  private def isUpCastableNumeric(catalystType: DataType, guessedType: DataType): Boolean = {

    // Both should be numeric types and `catalystType` should be wider.
    val isNumeric = Seq(catalystType, guessedType).forall(TypeCoercion.numericPrecedence.contains)

    // We use up-cast converter only if `guessedType` is narrower than `catalystType`.
    // If they are equal, it should falls back to a normal converter.
    val isUpCastable =
      TypeCoercion.numericPrecedence.lastIndexWhere(_ == catalystType) >
        TypeCoercion.numericPrecedence.lastIndexWhere(_ == guessedType)

    isNumeric && isUpCastable
  }

  def findUpCastConverter(
      guessedType: DataType,
      catalystType: DataType,
      updater: ParentContainerUpdater): Option[Converter with HasParentContainerUpdater] = {
    // These should be numeric types and up-castable.
    if (isUpCastableNumeric(catalystType, guessedType)) {
      val converter = makeNumericConverter(guessedType, updater)
      val valueUpdater = makeNumericUpdater(catalystType, updater)
      Some(converter(valueUpdater))
    } else {
      None
    }
  }
}
