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

package org.apache.spark.sql.types

import scala.language.implicitConversions
import scala.util.matching.Regex
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

import org.apache.spark.sql.catalyst.SqlLexical

/**
 * This is a data type parser that can be used to parse string representations of data types
 * provided in SQL queries. This parser is mixed in with DDLParser and SqlParser.
 */
private[sql] trait DataTypeParser extends StandardTokenParsers {

  // This is used to create a parser from a regex. We are using regexes for data type strings
  // since these strings can be also used as column names or field names.
  import lexical.Identifier
  implicit def regexToParser(regex: Regex): Parser[String] = acceptMatch(
    s"identifier matching regex ${regex}",
    { case Identifier(str) if regex.unapplySeq(str).isDefined => str }
  )

  protected lazy val primitiveType: Parser[DataType] =
    "(?i)string".r ^^^ StringType |
    "(?i)float".r ^^^ FloatType |
    "(?i)int".r ^^^ IntegerType |
    "(?i)tinyint".r ^^^ ByteType |
    "(?i)smallint".r ^^^ ShortType |
    "(?i)double".r ^^^ DoubleType |
    "(?i)bigint".r ^^^ LongType |
    "(?i)binary".r ^^^ BinaryType |
    "(?i)boolean".r ^^^ BooleanType |
    fixedDecimalType |
    "(?i)decimal".r ^^^ DecimalType.Unlimited |
    "(?i)date".r ^^^ DateType |
    "(?i)timestamp".r ^^^ TimestampType |
    varchar

  protected lazy val fixedDecimalType: Parser[DataType] =
    ("(?i)decimal".r ~> "(" ~> numericLit) ~ ("," ~> numericLit <~ ")") ^^ {
      case precision ~ scale =>
        DecimalType(precision.toInt, scale.toInt)
    }

  protected lazy val varchar: Parser[DataType] =
    "(?i)varchar".r ~> "(" ~> (numericLit <~ ")") ^^^ StringType

  protected lazy val arrayType: Parser[DataType] =
    "(?i)array".r ~> "<" ~> dataType <~ ">" ^^ {
      case tpe => ArrayType(tpe)
    }

  protected lazy val mapType: Parser[DataType] =
    "(?i)map".r ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    ident ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    ("(?i)struct".r ~> "<" ~> repsep(structField, ",") <~ ">"  ^^ {
      case fields => new StructType(fields.toArray)
    }) |
    ("(?i)struct".r ~ "<>" ^^^ StructType(Nil))

  protected lazy val dataType: Parser[DataType] =
    arrayType |
    mapType |
    structType |
    primitiveType

  def toDataType(dataTypeString: String): DataType = synchronized {
    phrase(dataType)(new lexical.Scanner(dataTypeString)) match {
      case Success(result, _) => result
      case failure: NoSuccess => throw new DataTypeException(failMessage(dataTypeString))
    }
  }

  private def failMessage(dataTypeString: String): String = {
    s"Unsupported dataType: $dataTypeString. If you have a struct and a field name of it has " +
      "any special characters, please use backticks (`) to quote that field name, e.g. `x+y`. " +
      "Please note that backtick itself is not supported in a field name."
  }
}

private[sql] object DataTypeParser {
  lazy val dataTypeParser = new DataTypeParser {
    override val lexical = new SqlLexical
  }

  def apply(dataTypeString: String): DataType = dataTypeParser.toDataType(dataTypeString)
}

/** The exception thrown from the [[DataTypeParser]]. */
private[sql] class DataTypeException(message: String) extends Exception(message)
