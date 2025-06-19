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

package org.apache.spark.sql.catalyst.parser

import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.sql.errors.DataTypeErrors
import org.apache.spark.sql.types._

/**
 * Parser that turns case class strings into datatypes. This is only here to maintain
 * compatibility with Parquet files written by Spark 1.1 and below.
 */
object LegacyTypeStringParser extends RegexParsers {

  protected lazy val primitiveType: Parser[DataType] =
    (
      "StringType" ^^^ StringType
        | "FloatType" ^^^ FloatType
        | "IntegerType" ^^^ IntegerType
        | "ByteType" ^^^ ByteType
        | "ShortType" ^^^ ShortType
        | "DoubleType" ^^^ DoubleType
        | "LongType" ^^^ LongType
        | "BinaryType" ^^^ BinaryType
        | "BooleanType" ^^^ BooleanType
        | "DateType" ^^^ DateType
        | "DecimalType()" ^^^ DecimalType.USER_DEFAULT
        | fixedDecimalType
        | "TimestampType" ^^^ TimestampType
    )

  protected lazy val fixedDecimalType: Parser[DataType] =
    ("DecimalType(" ~> "[0-9]+".r) ~ ("," ~> "[0-9]+".r <~ ")") ^^ { case precision ~ scale =>
      DecimalType(precision.toInt, scale.toInt)
    }

  protected lazy val arrayType: Parser[DataType] =
    "ArrayType" ~> "(" ~> dataType ~ "," ~ boolVal <~ ")" ^^ { case tpe ~ _ ~ containsNull =>
      ArrayType(tpe, containsNull)
    }

  protected lazy val mapType: Parser[DataType] =
    "MapType" ~> "(" ~> dataType ~ "," ~ dataType ~ "," ~ boolVal <~ ")" ^^ {
      case t1 ~ _ ~ t2 ~ _ ~ valueContainsNull => MapType(t1, t2, valueContainsNull)
    }

  protected lazy val structField: Parser[StructField] =
    ("StructField(" ~> "[a-zA-Z0-9_]*".r) ~ ("," ~> dataType) ~ ("," ~> boolVal <~ ")") ^^ {
      case name ~ tpe ~ nullable =>
        StructField(name, tpe, nullable = nullable)
    }

  protected lazy val boolVal: Parser[Boolean] =
    (
      "true" ^^^ true
        | "false" ^^^ false
    )

  protected lazy val structType: Parser[DataType] =
    "StructType\\([A-zA-z]*\\(".r ~> repsep(structField, ",") <~ "))" ^^ { case fields =>
      StructType(fields)
    }

  protected lazy val dataType: Parser[DataType] =
    (
      arrayType
        | mapType
        | structType
        | primitiveType
    )

  /**
   * Parses a string representation of a DataType.
   */
  def parseString(asString: String): DataType = parseAll(dataType, asString) match {
    case Success(result, _) => result
    case failure: NoSuccess =>
      throw DataTypeErrors.dataTypeUnsupportedError(asString, failure.toString)
  }
}
