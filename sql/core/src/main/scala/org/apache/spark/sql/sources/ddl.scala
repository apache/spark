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

import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.AbstractSparkSQLParser
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * A parser for foreign DDL commands.
 */
private[sql] class DDLParser extends AbstractSparkSQLParser with Logging {

  def apply(input: String, exceptionOnError: Boolean): Option[LogicalPlan] = {
    try {
      Some(apply(input))
    } catch {
      case ddlException: DDLException => throw ddlException
      case _ if !exceptionOnError => None
      case x: Throwable => throw x
    }
  }

  def parseType(input: String): DataType = {
    lexical.initialize(reservedWords)
    phrase(dataType)(new lexical.Scanner(input)) match {
      case Success(r, x) => r
      case x => throw new DDLException(s"Unsupported dataType: $x")
    }
  }

  // Keyword is a convention with AbstractSparkSQLParser, which will scan all of the `Keyword`
  // properties via reflection the class in runtime for constructing the SqlLexical object
  protected val CREATE = Keyword("CREATE")
  protected val TEMPORARY = Keyword("TEMPORARY")
  protected val TABLE = Keyword("TABLE")
  protected val IF = Keyword("IF")
  protected val NOT = Keyword("NOT")
  protected val EXISTS = Keyword("EXISTS")
  protected val USING = Keyword("USING")
  protected val OPTIONS = Keyword("OPTIONS")
  protected val DESCRIBE = Keyword("DESCRIBE")
  protected val EXTENDED = Keyword("EXTENDED")
  protected val AS = Keyword("AS")
  protected val COMMENT = Keyword("COMMENT")

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

  protected lazy val ddl: Parser[LogicalPlan] = createTable | describeTable

  protected def start: Parser[LogicalPlan] = ddl

  /**
   * `CREATE [TEMPORARY] TABLE avroTable [IF NOT EXISTS]
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")`
   * or
   * `CREATE [TEMPORARY] TABLE avroTable(intField int, stringField string...) [IF NOT EXISTS]
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")`
   * or
   * `CREATE [TEMPORARY] TABLE avroTable [IF NOT EXISTS]
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")`
   * AS SELECT ...
   */
  protected lazy val createTable: Parser[LogicalPlan] =
  (
    (CREATE ~> TEMPORARY.? <~ TABLE) ~ (IF ~> NOT <~ EXISTS).? ~ ident
      ~ (tableCols).? ~ (USING ~> className) ~ (OPTIONS ~> options) ~ (AS ~> restInput).? ^^ {
      case temp ~ allowExisting ~ tableName ~ columns ~ provider ~ opts ~ query =>
        if (temp.isDefined && allowExisting.isDefined) {
          throw new DDLException(
            "a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause.")
        }

        if (query.isDefined) {
          if (columns.isDefined) {
            throw new DDLException(
              "a CREATE TABLE AS SELECT statement does not allow column definitions.")
          }
          CreateTableUsingAsSelect(tableName,
            provider,
            temp.isDefined,
            opts,
            allowExisting.isDefined,
            query.get)
        } else {
          val userSpecifiedSchema = columns.flatMap(fields => Some(StructType(fields)))
          CreateTableUsing(
            tableName,
            userSpecifiedSchema,
            provider,
            temp.isDefined,
            opts,
            allowExisting.isDefined)
        }
      }
  )

  protected lazy val tableCols: Parser[Seq[StructField]] =  "(" ~> repsep(column, ",") <~ ")"

  /*
   * describe [extended] table avroTable
   * This will display all columns of table `avroTable` includes column_name,column_type,nullable
   */
  protected lazy val describeTable: Parser[LogicalPlan] =
    (DESCRIBE ~> opt(EXTENDED)) ~ (ident <~ ".").? ~ ident  ^^ {
      case e ~ db ~ tbl  =>
        val tblIdentifier = db match {
          case Some(dbName) =>
            Seq(dbName, tbl)
          case None =>
            Seq(tbl)
        }
        DescribeCommand(UnresolvedRelation(tblIdentifier, None), e.isDefined)
   }

  protected lazy val options: Parser[Map[String, String]] =
    "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

  protected lazy val className: Parser[String] = repsep(ident, ".") ^^ { case s => s.mkString(".")}

  protected lazy val pair: Parser[(String, String)] = ident ~ stringLit ^^ { case k ~ v => (k,v) }

  protected lazy val column: Parser[StructField] =
    ident ~ dataType ~ (COMMENT ~> stringLit).?  ^^ { case columnName ~ typ ~ cm =>
      val meta = cm match {
        case Some(comment) =>
          new MetadataBuilder().putString(COMMENT.str.toLowerCase(), comment).build()
        case None => Metadata.empty
      }
      StructField(columnName, typ, true, meta)
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
    case fields => StructType(fields)
    }) |
    (STRUCT ~> "<>" ^^ {
      case fields => StructType(Nil)
    })

  private[sql] lazy val dataType: Parser[DataType] =
    arrayType |
    mapType |
    structType |
    primitiveType
}

object ResolvedDataSource {
  def apply(
      sqlContext: SQLContext,
      userSpecifiedSchema: Option[StructType],
      provider: String,
      options: Map[String, String]): ResolvedDataSource = {
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
          case dataSource: org.apache.spark.sql.sources.RelationProvider =>
            sys.error(s"${clazz.getCanonicalName} does not allow user-specified schemas.")
        }
      }
      case None => {
        clazz.newInstance match {
          case dataSource: org.apache.spark.sql.sources.RelationProvider =>
            dataSource
              .asInstanceOf[org.apache.spark.sql.sources.RelationProvider]
              .createRelation(sqlContext, new CaseInsensitiveMap(options))
          case dataSource: org.apache.spark.sql.sources.SchemaRelationProvider =>
            sys.error(s"A schema needs to be specified when using ${clazz.getCanonicalName}.")
        }
      }
    }

    new ResolvedDataSource(clazz, relation)
  }

  def apply(
      sqlContext: SQLContext,
      provider: String,
      options: Map[String, String],
      data: DataFrame): ResolvedDataSource = {
    val loader = Utils.getContextOrSparkClassLoader
    val clazz: Class[_] = try loader.loadClass(provider) catch {
      case cnf: java.lang.ClassNotFoundException =>
        try loader.loadClass(provider + ".DefaultSource") catch {
          case cnf: java.lang.ClassNotFoundException =>
            sys.error(s"Failed to load class for data source: $provider")
        }
    }

    val relation = clazz.newInstance match {
      case dataSource: org.apache.spark.sql.sources.CreateableRelationProvider =>
        dataSource
          .asInstanceOf[org.apache.spark.sql.sources.CreateableRelationProvider]
          .createRelation(sqlContext, options, data)
      case _ =>
        sys.error(s"${clazz.getCanonicalName} does not allow create table as select.")
    }

    new ResolvedDataSource(clazz, relation)
  }
}

private[sql] case class ResolvedDataSource(provider: Class[_], relation: BaseRelation)

/**
 * Returned for the "DESCRIBE [EXTENDED] [dbName.]tableName" command.
 * @param table The table to be described.
 * @param isExtended True if "DESCRIBE EXTENDED" is used. Otherwise, false.
 *                   It is effective only when the table is a Hive table.
 */
private[sql] case class DescribeCommand(
    table: LogicalPlan,
    isExtended: Boolean) extends Command {
  override def output = Seq(
    // Column names are based on Hive.
    AttributeReference("col_name", StringType, nullable = false)(),
    AttributeReference("data_type", StringType, nullable = false)(),
    AttributeReference("comment", StringType, nullable = false)())
}

private[sql] case class CreateTableUsing(
    tableName: String,
    userSpecifiedSchema: Option[StructType],
    provider: String,
    temporary: Boolean,
    options: Map[String, String],
    allowExisting: Boolean) extends Command

private[sql] case class CreateTableUsingAsSelect(
    tableName: String,
    provider: String,
    temporary: Boolean,
    options: Map[String, String],
    allowExisting: Boolean,
    query: String) extends Command

private[sql] case class CreateTableUsingAsLogicalPlan(
    tableName: String,
    provider: String,
    temporary: Boolean,
    options: Map[String, String],
    allowExisting: Boolean,
    query: LogicalPlan) extends Command

private [sql] case class CreateTempTableUsing(
    tableName: String,
    userSpecifiedSchema: Option[StructType],
    provider: String,
    options: Map[String, String]) extends RunnableCommand {

  def run(sqlContext: SQLContext) = {
    val resolved = ResolvedDataSource(sqlContext, userSpecifiedSchema, provider, options)
    sqlContext.registerRDDAsTable(
      DataFrame(sqlContext, LogicalRelation(resolved.relation)), tableName)
    Seq.empty
  }
}

private [sql] case class CreateTempTableUsingAsSelect(
    tableName: String,
    provider: String,
    options: Map[String, String],
    query: LogicalPlan) extends RunnableCommand {

  def run(sqlContext: SQLContext) = {
    val df = DataFrame(sqlContext, query)
    val resolved = ResolvedDataSource(sqlContext, provider, options, df)
    sqlContext.registerRDDAsTable(
      DataFrame(sqlContext, LogicalRelation(resolved.relation)), tableName)

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

/**
 * The exception thrown from the DDL parser.
 * @param message
 */
protected[sql] class DDLException(message: String) extends Exception(message)
