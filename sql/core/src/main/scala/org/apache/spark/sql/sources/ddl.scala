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

package org.apache.spark.sql.sources

import scala.language.implicitConversions
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.{RegexParsers, PackratParsers}

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.util.Utils
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.SqlLexical

/**
 * A parser for foreign DDL commands.
 */
private[sql] class DDLParser extends StandardTokenParsers with PackratParsers with Logging {

  def apply(input: String): Option[LogicalPlan] = {
    phrase(ddl)(new lexical.Scanner(input)) match {
      case Success(r, x) => Some(r)
      case x =>
        logDebug(s"Not recognized as DDL: $x")
        None
    }
  }

  protected case class Keyword(str: String)

  protected implicit def asParser(k: Keyword): Parser[String] =
    lexical.allCaseVersions(k.str).map(x => x : Parser[String]).reduce(_ | _)

  // data types
  protected val STRING = Keyword("STRING")
  protected val DOUBLE = Keyword("DOUBLE")
  protected val BOOLEAN = Keyword("BOOLEAN")
  protected val FLOAT = Keyword("FLOAT")
  protected val INT = Keyword("INT")
  protected val TINYINT = Keyword("TINYINT")
  protected val SMALLINT = Keyword("SMALLINT")
  protected val BIGINT = Keyword("BIGINT")
  protected val BINARY = Keyword("BINARY")
  protected val DECIMAL = Keyword("DECIMAL")
  protected val DATE = Keyword("DATE")
  protected val TIMESTAMP = Keyword("TIMESTAMP")
  protected val VARCHAR = Keyword("VARCHAR")

  protected val CREATE = Keyword("CREATE")
  protected val TEMPORARY = Keyword("TEMPORARY")
  protected val TABLE = Keyword("TABLE")
  protected val USING = Keyword("USING")
  protected val OPTIONS = Keyword("OPTIONS")

  // Use reflection to find the reserved words defined in this class.
  protected val reservedWords =
    this.getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = new SqlLexical(reservedWords)

  protected lazy val ddl: Parser[LogicalPlan] = createTable

  /**
   * `CREATE TEMPORARY TABLE avroTable
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")`
   * or
   * `CREATE TEMPORARY TABLE avroTable(intField int, stringField string...)
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")`
   */
  protected lazy val createTable: Parser[LogicalPlan] =
  (  CREATE ~ TEMPORARY ~ TABLE ~> ident ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case tableName ~ provider ~ opts =>
        CreateTableUsing(tableName, Seq.empty, provider, opts)
    }
  |
    CREATE ~ TEMPORARY ~ TABLE ~> ident
      ~ tableCols  ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case tableName ~ tableColumns ~ provider ~ opts =>
      CreateTableUsing(tableName, tableColumns, provider, opts)
    }
  )

  protected lazy val metastoreTypes = new MetastoreTypes

  protected lazy val tableCols: Parser[Seq[StructField]] =  "(" ~> repsep(column, ",") <~ ")"

  protected lazy val options: Parser[Map[String, String]] =
    "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

  protected lazy val className: Parser[String] = repsep(ident, ".") ^^ { case s => s.mkString(".")}

  protected lazy val pair: Parser[(String, String)] = ident ~ stringLit ^^ { case k ~ v => (k,v) }

  protected lazy val column: Parser[StructField] =
    ident ~  ident ^^ { case name ~ typ =>
      StructField(name, metastoreTypes.toDataType(typ))
    }
}

/**
 * :: DeveloperApi ::
 * Provides a parser for data types.
 */
@DeveloperApi
private[sql] class MetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
      "float" ^^^ FloatType |
      "int" ^^^ IntegerType |
      "tinyint" ^^^ ByteType |
      "smallint" ^^^ ShortType |
      "double" ^^^ DoubleType |
      "bigint" ^^^ LongType |
      "binary" ^^^ BinaryType |
      "boolean" ^^^ BooleanType |
      fixedDecimalType |                     // Hive 0.13+ decimal with precision/scale
      "decimal" ^^^ DecimalType.Unlimited |  // Hive 0.12 decimal with no precision/scale
      "date" ^^^ DateType |
      "timestamp" ^^^ TimestampType |
      "varchar\\((\\d+)\\)".r ^^^ StringType

  protected lazy val fixedDecimalType: Parser[DataType] =
    ("decimal" ~> "(" ~> "\\d+".r) ~ ("," ~> "\\d+".r <~ ")") ^^ {
      case precision ~ scale =>
        DecimalType(precision.toInt, scale.toInt)
    }

  protected lazy val arrayType: Parser[DataType] =
    "array" ~> "<" ~> dataType <~ ">" ^^ {
      case tpe => ArrayType(tpe)
    }

  protected lazy val mapType: Parser[DataType] =
    "map" ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    "[a-zA-Z0-9_]*".r ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    "struct" ~> "<" ~> repsep(structField,",") <~ ">"  ^^ {
      case fields => new StructType(fields)
    }

  private[sql] lazy val dataType: Parser[DataType] =
    arrayType |
      mapType |
      structType |
      primitiveType

  def toDataType(metastoreType: String): DataType = parseAll(dataType, metastoreType) match {
    case Success(result, _) => result
    case failure: NoSuccess => sys.error(s"Unsupported dataType: $metastoreType")
  }

  def toMetastoreType(dt: DataType): String = dt match {
    case ArrayType(elementType, _) => s"array<${toMetastoreType(elementType)}>"
    case StructType(fields) =>
      s"struct<${fields.map(f => s"${f.name}:${toMetastoreType(f.dataType)}").mkString(",")}>"
    case MapType(keyType, valueType, _) =>
      s"map<${toMetastoreType(keyType)},${toMetastoreType(valueType)}>"
    case StringType => "string"
    case FloatType => "float"
    case IntegerType => "int"
    case ByteType => "tinyint"
    case ShortType => "smallint"
    case DoubleType => "double"
    case LongType => "bigint"
    case BinaryType => "binary"
    case BooleanType => "boolean"
    case DateType => "date"
    case d: DecimalType => "decimal"
    case TimestampType => "timestamp"
    case NullType => "void"
    case udt: UserDefinedType[_] => toMetastoreType(udt.sqlType)
  }
}

private[sql] case class CreateTableUsing(
    tableName: String,
    tableCols: Seq[StructField],
    provider: String,
    options: Map[String, String]) extends RunnableCommand {

  def run(sqlContext: SQLContext) = {
    val loader = Utils.getContextOrSparkClassLoader
    val clazz: Class[_] = try loader.loadClass(provider) catch {
      case cnf: java.lang.ClassNotFoundException =>
        try loader.loadClass(provider + ".DefaultSource") catch {
          case cnf: java.lang.ClassNotFoundException =>
            sys.error(s"Failed to load class for data source: $provider")
        }
    }
    val dataSource =
      clazz.newInstance().asInstanceOf[org.apache.spark.sql.sources.SchemaRelationProvider]
    val relation = dataSource.createRelation(
      sqlContext, new CaseInsensitiveMap(options), Some(StructType(tableCols)))

    sqlContext.baseRelationToSchemaRDD(relation).registerTempTable(tableName)
    Seq.empty
  }
}

/**
 * Builds a map in which keys are case insensitive
 */
protected class CaseInsensitiveMap(map: Map[String, String]) extends Map[String, String] {

  val baseMap = map.map(kv => kv.copy(_1 = kv._1.toLowerCase))

  override def get(k: String): Option[String] = baseMap.get(k.toLowerCase)

  override def + [B1 >: String](kv: (String, B1)): Map[String, B1] =
    baseMap + kv.copy(_1 = kv._1.toLowerCase)

  override def iterator: Iterator[(String, String)] = baseMap.iterator

  override def -(key: String): Map[String, String] = baseMap - key.toLowerCase()
}
