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

import scala.language.{existentials, implicitConversions}
import scala.util.matching.Regex

import org.apache.hadoop.fs.Path

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.AbstractSparkSQLParser
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Row}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}
import org.apache.spark.util.Utils

/**
 * A parser for foreign DDL commands.
 */
private[sql] class DDLParser(
    parseQuery: String => LogicalPlan)
  extends AbstractSparkSQLParser with DataTypeParser with Logging {

  def parse(input: String, exceptionOnError: Boolean): LogicalPlan = {
    try {
      parse(input)
    } catch {
      case ddlException: DDLException => throw ddlException
      case _ if !exceptionOnError => parseQuery(input)
      case x: Throwable => throw x
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
  protected val REFRESH = Keyword("REFRESH")

  protected lazy val ddl: Parser[LogicalPlan] = createTable | describeTable | refreshTable

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
    // TODO: Support database.table.
    (CREATE ~> TEMPORARY.? <~ TABLE) ~ (IF ~> NOT <~ EXISTS).? ~ ident ~
      tableCols.? ~ (USING ~> className) ~ (OPTIONS ~> options).? ~ (AS ~> restInput).? ^^ {
      case temp ~ allowExisting ~ tableName ~ columns ~ provider ~ opts ~ query =>
        if (temp.isDefined && allowExisting.isDefined) {
          throw new DDLException(
            "a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause.")
        }

        val options = opts.getOrElse(Map.empty[String, String])
        if (query.isDefined) {
          if (columns.isDefined) {
            throw new DDLException(
              "a CREATE TABLE AS SELECT statement does not allow column definitions.")
          }
          // When IF NOT EXISTS clause appears in the query, the save mode will be ignore.
          val mode = if (allowExisting.isDefined) {
            SaveMode.Ignore
          } else if (temp.isDefined) {
            SaveMode.Overwrite
          } else {
            SaveMode.ErrorIfExists
          }

          val queryPlan = parseQuery(query.get)
          CreateTableUsingAsSelect(tableName,
            provider,
            temp.isDefined,
            Array.empty[String],
            mode,
            options,
            queryPlan)
        } else {
          val userSpecifiedSchema = columns.flatMap(fields => Some(StructType(fields)))
          CreateTableUsing(
            tableName,
            userSpecifiedSchema,
            provider,
            temp.isDefined,
            options,
            allowExisting.isDefined,
            managedIfNoPath = false)
        }
    }

  protected lazy val tableCols: Parser[Seq[StructField]] = "(" ~> repsep(column, ",") <~ ")"

  /*
   * describe [extended] table avroTable
   * This will display all columns of table `avroTable` includes column_name,column_type,comment
   */
  protected lazy val describeTable: Parser[LogicalPlan] =
    (DESCRIBE ~> opt(EXTENDED)) ~ (ident <~ ".").? ~ ident  ^^ {
      case e ~ db ~ tbl =>
        val tblIdentifier = db match {
          case Some(dbName) =>
            Seq(dbName, tbl)
          case None =>
            Seq(tbl)
        }
        DescribeCommand(UnresolvedRelation(tblIdentifier, None), e.isDefined)
   }

  protected lazy val refreshTable: Parser[LogicalPlan] =
    REFRESH ~> TABLE ~> (ident <~ ".").? ~ ident ^^ {
      case maybeDatabaseName ~ tableName =>
        RefreshTable(maybeDatabaseName.getOrElse("default"), tableName)
    }

  protected lazy val options: Parser[Map[String, String]] =
    "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

  protected lazy val className: Parser[String] = repsep(ident, ".") ^^ { case s => s.mkString(".")}

  override implicit def regexToParser(regex: Regex): Parser[String] = acceptMatch(
    s"identifier matching regex $regex", {
      case lexical.Identifier(str) if regex.unapplySeq(str).isDefined => str
      case lexical.Keyword(str) if regex.unapplySeq(str).isDefined => str
    }
  )

  protected lazy val optionPart: Parser[String] = "[_a-zA-Z][_a-zA-Z0-9]*".r ^^ {
    case name => name
  }

  protected lazy val optionName: Parser[String] = repsep(optionPart, ".") ^^ {
    case parts => parts.mkString(".")
  }

  protected lazy val pair: Parser[(String, String)] =
    optionName ~ stringLit ^^ { case k ~ v => (k, v) }

  protected lazy val column: Parser[StructField] =
    ident ~ dataType ~ (COMMENT ~> stringLit).?  ^^ { case columnName ~ typ ~ cm =>
      val meta = cm match {
        case Some(comment) =>
          new MetadataBuilder().putString(COMMENT.str.toLowerCase, comment).build()
        case None => Metadata.empty
      }

      StructField(columnName, typ, nullable = true, meta)
    }
}

private[sql] object ResolvedDataSource {

  private val builtinSources = Map(
    "jdbc" -> "org.apache.spark.sql.jdbc.DefaultSource",
    "json" -> "org.apache.spark.sql.json.DefaultSource",
    "parquet" -> "org.apache.spark.sql.parquet.DefaultSource",
    "orc" -> "org.apache.spark.sql.hive.orc.DefaultSource"
  )

  /** Given a provider name, look up the data source class definition. */
  def lookupDataSource(provider: String): Class[_] = {
    val loader = Utils.getContextOrSparkClassLoader

    if (builtinSources.contains(provider)) {
      return loader.loadClass(builtinSources(provider))
    }

    try {
      loader.loadClass(provider)
    } catch {
      case cnf: java.lang.ClassNotFoundException =>
        try {
          loader.loadClass(provider + ".DefaultSource")
        } catch {
          case cnf: java.lang.ClassNotFoundException =>
            if (provider.startsWith("org.apache.spark.sql.hive.orc")) {
              sys.error("The ORC data source must be used with Hive support enabled.")
            } else {
              sys.error(s"Failed to load class for data source: $provider")
            }
        }
    }
  }

  /** Create a [[ResolvedDataSource]] for reading data in. */
  def apply(
      sqlContext: SQLContext,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String],
      provider: String,
      options: Map[String, String]): ResolvedDataSource = {
    val clazz: Class[_] = lookupDataSource(provider)
    def className: String = clazz.getCanonicalName
    val relation = userSpecifiedSchema match {
      case Some(schema: StructType) => clazz.newInstance() match {
        case dataSource: SchemaRelationProvider =>
          dataSource.createRelation(sqlContext, new CaseInsensitiveMap(options), schema)
        case dataSource: HadoopFsRelationProvider =>
          val maybePartitionsSchema = if (partitionColumns.isEmpty) {
            None
          } else {
            Some(partitionColumnsSchema(schema, partitionColumns))
          }

          val caseInsensitiveOptions = new CaseInsensitiveMap(options)
          val paths = {
            val patternPath = new Path(caseInsensitiveOptions("path"))
            SparkHadoopUtil.get.globPath(patternPath).map(_.toString).toArray
          }

          val dataSchema =
            StructType(schema.filterNot(f => partitionColumns.contains(f.name))).asNullable

          dataSource.createRelation(
            sqlContext,
            paths,
            Some(dataSchema),
            maybePartitionsSchema,
            caseInsensitiveOptions)
        case dataSource: org.apache.spark.sql.sources.RelationProvider =>
          throw new AnalysisException(s"$className does not allow user-specified schemas.")
        case _ =>
          throw new AnalysisException(s"$className is not a RelationProvider.")
      }

      case None => clazz.newInstance() match {
        case dataSource: RelationProvider =>
          dataSource.createRelation(sqlContext, new CaseInsensitiveMap(options))
        case dataSource: HadoopFsRelationProvider =>
          val caseInsensitiveOptions = new CaseInsensitiveMap(options)
          val paths = {
            val patternPath = new Path(caseInsensitiveOptions("path"))
            SparkHadoopUtil.get.globPath(patternPath).map(_.toString).toArray
          }
          dataSource.createRelation(sqlContext, paths, None, None, caseInsensitiveOptions)
        case dataSource: org.apache.spark.sql.sources.SchemaRelationProvider =>
          throw new AnalysisException(
            s"A schema needs to be specified when using $className.")
        case _ =>
          throw new AnalysisException(
            s"$className is neither a RelationProvider nor a FSBasedRelationProvider.")
      }
    }
    new ResolvedDataSource(clazz, relation)
  }

  private def partitionColumnsSchema(
      schema: StructType,
      partitionColumns: Array[String]): StructType = {
    StructType(partitionColumns.map { col =>
      schema.find(_.name == col).getOrElse {
        throw new RuntimeException(s"Partition column $col not found in schema $schema")
      }
    }).asNullable
  }

  /** Create a [[ResolvedDataSource]] for saving the content of the given [[DataFrame]]. */
  def apply(
      sqlContext: SQLContext,
      provider: String,
      partitionColumns: Array[String],
      mode: SaveMode,
      options: Map[String, String],
      data: DataFrame): ResolvedDataSource = {
    val clazz: Class[_] = lookupDataSource(provider)
    val relation = clazz.newInstance() match {
      case dataSource: CreatableRelationProvider =>
        dataSource.createRelation(sqlContext, mode, options, data)
      case dataSource: HadoopFsRelationProvider =>
        // Don't glob path for the write path.  The contracts here are:
        //  1. Only one output path can be specified on the write path;
        //  2. Output path must be a legal HDFS style file system path;
        //  3. It's OK that the output path doesn't exist yet;
        val caseInsensitiveOptions = new CaseInsensitiveMap(options)
        val outputPath = {
          val path = new Path(caseInsensitiveOptions("path"))
          val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
          path.makeQualified(fs.getUri, fs.getWorkingDirectory)
        }
        val dataSchema = StructType(data.schema.filterNot(f => partitionColumns.contains(f.name)))
        val r = dataSource.createRelation(
          sqlContext,
          Array(outputPath.toString),
          Some(dataSchema.asNullable),
          Some(partitionColumnsSchema(data.schema, partitionColumns)),
          caseInsensitiveOptions)

        // For partitioned relation r, r.schema's column ordering can be different from the column
        // ordering of data.logicalPlan (partition columns are all moved after data column).  This
        // will be adjusted within InsertIntoHadoopFsRelation.
        sqlContext.executePlan(
          InsertIntoHadoopFsRelation(
            r,
            data.logicalPlan,
            mode)).toRdd
        r
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
    isExtended: Boolean) extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = Seq.empty
  override val output: Seq[Attribute] = Seq(
    // Column names are based on Hive.
    AttributeReference("col_name", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "name of the column").build())(),
    AttributeReference("data_type", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "data type of the column").build())(),
    AttributeReference("comment", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "comment of the column").build())())
}

/**
  * Used to represent the operation of create table using a data source.
  * @param allowExisting If it is true, we will do nothing when the table already exists.
  *                      If it is false, an exception will be thrown
  */
private[sql] case class CreateTableUsing(
    tableName: String,
    userSpecifiedSchema: Option[StructType],
    provider: String,
    temporary: Boolean,
    options: Map[String, String],
    allowExisting: Boolean,
    managedIfNoPath: Boolean) extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
}

/**
 * A node used to support CTAS statements and saveAsTable for the data source API.
 * This node is a [[UnaryNode]] instead of a [[Command]] because we want the analyzer
 * can analyze the logical plan that will be used to populate the table.
 * So, [[PreWriteCheck]] can detect cases that are not allowed.
 */
private[sql] case class CreateTableUsingAsSelect(
    tableName: String,
    provider: String,
    temporary: Boolean,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = Seq.empty[Attribute]
  // TODO: Override resolved after we support databaseName.
  // override lazy val resolved = databaseName != None && childrenResolved
}

private[sql] case class CreateTempTableUsing(
    tableName: String,
    userSpecifiedSchema: Option[StructType],
    provider: String,
    options: Map[String, String]) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val resolved = ResolvedDataSource(
      sqlContext, userSpecifiedSchema, Array.empty[String], provider, options)
    sqlContext.registerDataFrameAsTable(
      DataFrame(sqlContext, LogicalRelation(resolved.relation)), tableName)
    Seq.empty
  }
}

private[sql] case class CreateTempTableUsingAsSelect(
    tableName: String,
    provider: String,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val df = DataFrame(sqlContext, query)
    val resolved = ResolvedDataSource(sqlContext, provider, partitionColumns, mode, options, df)
    sqlContext.registerDataFrameAsTable(
      DataFrame(sqlContext, LogicalRelation(resolved.relation)), tableName)

    Seq.empty
  }
}

private[sql] case class RefreshTable(databaseName: String, tableName: String)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    // Refresh the given table's metadata first.
    sqlContext.catalog.refreshTable(databaseName, tableName)

    // If this table is cached as a InMemoryColumnarRelation, drop the original
    // cached version and make the new version cached lazily.
    val logicalPlan = sqlContext.catalog.lookupRelation(Seq(databaseName, tableName))
    // Use lookupCachedData directly since RefreshTable also takes databaseName.
    val isCached = sqlContext.cacheManager.lookupCachedData(logicalPlan).nonEmpty
    if (isCached) {
      // Create a data frame to represent the table.
      // TODO: Use uncacheTable once it supports database name.
      val df = DataFrame(sqlContext, logicalPlan)
      // Uncache the logicalPlan.
      sqlContext.cacheManager.tryUncacheQuery(df, blocking = true)
      // Cache it again.
      sqlContext.cacheManager.cacheQuery(df, Some(tableName))
    }

    Seq.empty[Row]
  }
}

/**
 * Builds a map in which keys are case insensitive
 */
protected[sql] class CaseInsensitiveMap(map: Map[String, String]) extends Map[String, String]
  with Serializable {

  val baseMap = map.map(kv => kv.copy(_1 = kv._1.toLowerCase))

  override def get(k: String): Option[String] = baseMap.get(k.toLowerCase)

  override def + [B1 >: String](kv: (String, B1)): Map[String, B1] =
    baseMap + kv.copy(_1 = kv._1.toLowerCase)

  override def iterator: Iterator[(String, String)] = baseMap.iterator

  override def -(key: String): Map[String, String] = baseMap - key.toLowerCase
}

/**
 * The exception thrown from the DDL parser.
 */
protected[sql] class DDLException(message: String) extends Exception(message)
