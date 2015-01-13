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
import scala.util.parsing.combinator.PackratParsers

import org.apache.spark.Logging
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

  def parseType(input: String): DataType = {
    phrase(dataType)(new lexical.Scanner(input)) match {
      case Success(r, x) => r
      case x =>
        sys.error(s"Unsupported dataType: $x")
    }
  }

  protected case class Keyword(str: String)

  protected implicit def asParser(k: Keyword): Parser[String] =
    lexical.allCaseVersions(k.str).map(x => x : Parser[String]).reduce(_ | _)

  protected val CREATE = Keyword("CREATE")
  protected val TEMPORARY = Keyword("TEMPORARY")
  protected val TABLE = Keyword("TABLE")
  protected val USING = Keyword("USING")
  protected val OPTIONS = Keyword("OPTIONS")

  // Data types.
  protected val STRING = Keyword("STRING")
  protected val BINARY = Keyword("BINARY")
  protected val BOOLEAN = Keyword("BOOLEAN")
  protected val TINYINT = Keyword("TINYINT")
  protected val SMALLINT = Keyword("SMALLINT")
  protected val INT = Keyword("INT")
  protected val BIGINT = Keyword("BIGINT")
  protected val FLOAT = Keyword("FLOAT")
  protected val DOUBLE = Keyword("DOUBLE")
  protected val DECIMAL = Keyword("DECIMAL")
  protected val DATE = Keyword("DATE")
  protected val TIMESTAMP = Keyword("TIMESTAMP")
  protected val VARCHAR = Keyword("VARCHAR")
  protected val ARRAY = Keyword("ARRAY")
  protected val MAP = Keyword("MAP")
  protected val STRUCT = Keyword("STRUCT")

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
  (
    CREATE ~ TEMPORARY ~ TABLE ~> ident
      ~ (tableCols).? ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case tableName ~ columns ~ provider ~ opts =>
        val userSpecifiedSchema = columns.flatMap(fields => Some(StructType(fields)))
        CreateTableUsing(tableName, userSpecifiedSchema, provider, opts)
    }
  )

  protected lazy val tableCols: Parser[Seq[StructField]] =  "(" ~> repsep(column, ",") <~ ")"

  protected lazy val options: Parser[Map[String, String]] =
    "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

  protected lazy val className: Parser[String] = repsep(ident, ".") ^^ { case s => s.mkString(".")}

  protected lazy val pair: Parser[(String, String)] = ident ~ stringLit ^^ { case k ~ v => (k,v) }

  protected lazy val column: Parser[StructField] =
    ident ~ dataType ^^ { case columnName ~ typ =>
      StructField(columnName, typ)
    }

  protected lazy val primitiveType: Parser[DataType] =
    STRING ^^^ StringType |
    BINARY ^^^ BinaryType |
    BOOLEAN ^^^ BooleanType |
    TINYINT ^^^ ByteType |
    SMALLINT ^^^ ShortType |
    INT ^^^ IntegerType |
    BIGINT ^^^ LongType |
    FLOAT ^^^ FloatType |
    DOUBLE ^^^ DoubleType |
    fixedDecimalType |                   // decimal with precision/scale
    DECIMAL ^^^ DecimalType.Unlimited |  // decimal with no precision/scale
    DATE ^^^ DateType |
    TIMESTAMP ^^^ TimestampType |
    VARCHAR ~ "(" ~ numericLit ~ ")" ^^^ StringType

  protected lazy val fixedDecimalType: Parser[DataType] =
    (DECIMAL ~ "(" ~> numericLit) ~ ("," ~> numericLit <~ ")") ^^ {
      case precision ~ scale => DecimalType(precision.toInt, scale.toInt)
    }

  protected lazy val arrayType: Parser[DataType] =
    ARRAY ~> "<" ~> dataType <~ ">" ^^ {
      case tpe => ArrayType(tpe)
    }

  protected lazy val mapType: Parser[DataType] =
    MAP ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    ident ~ ":" ~ dataType ^^ {
      case fieldName ~ _ ~ tpe => StructField(fieldName, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    (STRUCT ~> "<" ~> repsep(structField, ",") <~ ">" ^^ {
    case fields => new StructType(fields)
    }) |
    (STRUCT ~> "<>" ^^ {
      case fields => new StructType(Nil)
    })

  private[sql] lazy val dataType: Parser[DataType] =
    arrayType |
    mapType |
    structType |
    primitiveType
}

private[sql] case class CreateTableUsing(
    tableName: String,
    userSpecifiedSchema: Option[StructType],
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

    val relation = userSpecifiedSchema match {
      case Some(schema: StructType) => {
        clazz.newInstance match {
          case dataSource: org.apache.spark.sql.sources.SchemaRelationProvider =>
            dataSource
              .asInstanceOf[org.apache.spark.sql.sources.SchemaRelationProvider]
              .createRelation(sqlContext, new CaseInsensitiveMap(options), schema)
          case _ =>
            sys.error(s"${clazz.getCanonicalName} should extend SchemaRelationProvider.")
        }
      }
      case None => {
        clazz.newInstance match {
          case dataSource: org.apache.spark.sql.sources.RelationProvider  =>
            dataSource
              .asInstanceOf[org.apache.spark.sql.sources.RelationProvider]
              .createRelation(sqlContext, new CaseInsensitiveMap(options))
          case _ =>
            sys.error(s"${clazz.getCanonicalName} should extend RelationProvider.")
        }
      }
    }

    sqlContext.baseRelationToSchemaRDD(relation).registerTempTable(tableName)
    Seq.empty
  }
}

/**
 * Builds a map in which keys are case insensitive
 */
protected class CaseInsensitiveMap(map: Map[String, String]) extends Map[String, String] 
  with Serializable {

  val baseMap = map.map(kv => kv.copy(_1 = kv._1.toLowerCase))

  override def get(k: String): Option[String] = baseMap.get(k.toLowerCase)

  override def + [B1 >: String](kv: (String, B1)): Map[String, B1] =
    baseMap + kv.copy(_1 = kv._1.toLowerCase)

  override def iterator: Iterator[(String, String)] = baseMap.iterator

  override def -(key: String): Map[String, String] = baseMap - key.toLowerCase()
}
