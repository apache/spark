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

package org.apache.spark.sql

import java.sql.Timestamp

import org.scalacheck.{Arbitrary, Gen}

import org.apache.spark.sql.types._

/**
 * ScalaCheck random data generators for Spark SQL DataTypes.
 */
object RandomDataGenerator {

  /**
   * Returns a function which generates random values for the given [[DataType]], or `None` if no
   * random data generator is defined for that data type. The generated values will use an external
   * representation of the data type; for example, the random generator for [[DateType]] will return
   * instances of [[java.sql.Date]] and the generator for [[StructType]] will return a
   * [[org.apache.spark.Row]].
   *
   * @param dataType the type to generate values for
   * @param nullable whether null values should be generated
   * @return a ScalaCheck [[Gen]] which can be used to produce random values.
   */
  def forType(
      dataType: DataType,
      nullable: Boolean = true): Option[Gen[Any]] = {
    val valueGenerator: Option[Gen[Any]] = dataType match {
      case StringType => Some(Arbitrary.arbitrary[String])
      case BinaryType => Some(Gen.listOf(Arbitrary.arbitrary[Byte]).map(_.toArray))
      case BooleanType => Some(Arbitrary.arbitrary[Boolean])
      case DateType => Some(Arbitrary.arbitrary[Int].suchThat(_ >= 0).map(new java.sql.Date(_)))
      case DoubleType => Some(Arbitrary.arbitrary[Double])
      case FloatType => Some(Arbitrary.arbitrary[Float])
      case ByteType => Some(Arbitrary.arbitrary[Byte])
      case IntegerType => Some(Arbitrary.arbitrary[Int])
      case LongType => Some(Arbitrary.arbitrary[Long])
      case ShortType => Some(Arbitrary.arbitrary[Short])
      case NullType => Some(Gen.const[Any](null))
      case TimestampType => Some(Arbitrary.arbitrary[Long].suchThat(_ >= 0).map(new Timestamp(_)))
      case DecimalType.Unlimited => Some(Arbitrary.arbitrary[BigDecimal])
      case ArrayType(elementType, containsNull) => {
        forType(elementType, nullable = containsNull).map { elementGen =>
          Gen.listOf(elementGen).map(_.toArray)
        }
      }
      case MapType(keyType, valueType, valueContainsNull) => {
        for (
          keyGenerator <- forType(keyType, nullable = false);
          valueGenerator <- forType(valueType, nullable = valueContainsNull)
          // Scala's BigDecimal.hashCode can lead to OutOfMemoryError on Scala 2.10 (see SI-6173)
          // and Spark can hit NumberFormatException errors converting certain BigDecimals
          // (SPARK-8802). For these reasons, we don't support generation of maps with decimal keys.
          if !keyType.isInstanceOf[DecimalType]
        ) yield {
          Gen.listOf(Gen.zip(keyGenerator, valueGenerator)).map(_.toMap)
        }
      }
      case StructType(fields) => {
        val maybeFieldGenerators: Seq[Option[Gen[Any]]] = fields.map { field =>
          forType(field.dataType, nullable = field.nullable)
        }
        if (maybeFieldGenerators.forall(_.isDefined)) {
          Some(Gen.sequence[Seq[Any], Any](maybeFieldGenerators.flatten).map(vs => Row.fromSeq(vs)))
        } else {
          None
        }
      }
      case unsupportedType => None
    }
    if (nullable) {
      valueGenerator.map(Gen.oneOf(_, Gen.const[Any](null)))
    } else {
      valueGenerator
    }
  }
}
