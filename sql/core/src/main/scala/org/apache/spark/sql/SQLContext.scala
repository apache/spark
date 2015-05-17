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

import java.beans.Introspector
import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import com.google.common.reflect.TypeToken

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.errors.DialectException
import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.Dialect
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection, expressions}
import org.apache.spark.sql.execution.{Filter, _}
import org.apache.spark.sql.jdbc.{JDBCPartition, JDBCPartitioningInfo, JDBCRelation}
import org.apache.spark.sql.json._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils
import org.apache.spark.{Partition, SparkContext}

/**
 * Currently we support the default dialect named "sql", associated with the class
 * [[DefaultDialect]]
 *
 * And we can also provide custom SQL Dialect, for example in Spark SQL CLI:
 * {{{
 *-- switch to "hiveql" dialect
 *   spark-sql>SET spark.sql.dialect=hiveql;
 *   spark-sql>SELECT * FROM src LIMIT 1;
 *
 *-- switch to "sql" dialect
 *   spark-sql>SET spark.sql.dialect=sql;
 *   spark-sql>SELECT * FROM src LIMIT 1;
 *
 *-- register the new SQL dialect
 *   spark-sql> SET spark.sql.dialect=com.xxx.xxx.SQL99Dialect;
 *   spark-sql> SELECT * FROM src LIMIT 1;
 *
 *-- register the non-exist SQL dialect
 *   spark-sql> SET spark.sql.dialect=NotExistedClass;
 *   spark-sql> SELECT * FROM src LIMIT 1;
 *
 *-- Exception will be thrown and switch to dialect
 *-- "sql" (for SQLContext) or 
 *-- "hiveql" (for HiveContext)
 * }}}
 */
private[spark] class DefaultDialect extends Dialect {
  @transient
  protected val sqlParser = new catalyst.SqlParser

  override def parse(sqlText: String): LogicalPlan = {
    sqlParser.parse(sqlText)
  }
}

/**
 * The entry point for working with structured data (rows and columns) in Spark.  Allows the
 * creation of [[DataFrame]] objects as well as the execution of SQL queries.
 *
 * @groupname basic Basic Operations
 * @groupname ddl_ops Persistent Catalog DDL
 * @groupname cachemgmt Cached Table Management
 * @groupname genericdata Generic Data Sources
 * @groupname specificdata Specific Data Sources
 * @groupname config Configuration
 * @groupname dataframes Custom DataFrame Creation
 * @groupname Ungrouped Support functions for language integrated queries.
 */
class SQLContext(@transient val sparkContext: SparkContext)
  extends org.apache.spark.Logging
  with Serializable {

  self =>

  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  /**
   * @return Spark SQL configuration
   */
  protected[sql] def conf = tlSession.get().conf

  /**
   * Set Spark SQL configuration properties.
   *
   * @group config
   */
  def setConf(props: Properties): Unit = conf.setConf(props)

  /**
   * Set the given Spark SQL configuration property.
   *
   * @group config
   */
  def setConf(key: String, value: String): Unit = conf.setConf(key, value)

  /**
   * Return the value of Spark SQL configuration property for the given key.
   *
   * @group config
   */
  def getConf(key: String): String = conf.getConf(key)

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue`.
   *
   * @group config
   */
  def getConf(key: String, defaultValue: String): String = conf.getConf(key, defaultValue)

  /**
   * Return all the configuration properties that have been set (i.e. not the default).
   * This creates a new copy of the config properties in the form of a Map.
   *
   * @group config
   */
  def getAllConfs: immutable.Map[String, String] = conf.getAllConfs

  // TODO how to handle the temp table per user session?
  @transient
  protected[sql] lazy val catalog: Catalog = new SimpleCatalog(true)

  // TODO how to handle the temp function per user session?
  @transient
  protected[sql] lazy val functionRegistry: FunctionRegistry = new SimpleFunctionRegistry(true)

  @transient
  protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, caseSensitive = true) {
      override val extendedResolutionRules =
        ExtractPythonUdfs ::
        sources.PreInsertCastAndRename ::
        Nil

      override val extendedCheckRules = Seq(
        sources.PreWriteCheck(catalog)
      )
    }

  @transient
  protected[sql] lazy val optimizer: Optimizer = DefaultOptimizer

  @transient
  protected[sql] val ddlParser = new DDLParser(sqlParser.parse(_))

  @transient
  protected[sql] val sqlParser = new SparkSQLParser(getSQLDialect().parse(_))

  protected[sql] def getSQLDialect(): Dialect = {
    try {
      val clazz = Utils.classForName(dialectClassName)
      clazz.newInstance().asInstanceOf[Dialect]
    } catch {
      case NonFatal(e) =>
        // Since we didn't find the available SQL Dialect, it will fail even for SET command:
        // SET spark.sql.dialect=sql; Let's reset as default dialect automatically.
        val dialect = conf.dialect
        // reset the sql dialect
        conf.unsetConf(SQLConf.DIALECT)
        // throw out the exception, and the default sql dialect will take effect for next query.
        throw new DialectException(
          s"""Instantiating dialect '$dialect' failed.
             |Reverting to default dialect '${conf.dialect}'""".stripMargin, e)
    }
  }

  protected[sql] def parseSql(sql: String): LogicalPlan = ddlParser.parse(sql, false)

  protected[sql] def executeSql(sql: String): this.QueryExecution = executePlan(parseSql(sql))

  protected[sql] def executePlan(plan: LogicalPlan) = new this.QueryExecution(plan)

  @transient
  protected[sql] val tlSession = new ThreadLocal[SQLSession]() {
    override def initialValue: SQLSession = defaultSession
  }

  @transient
  protected[sql] val defaultSession = createSession()

  protected[sql] def dialectClassName = if (conf.dialect == "sql") {
    classOf[DefaultDialect].getCanonicalName
  } else {
    conf.dialect
  }

  sparkContext.getConf.getAll.foreach {
    case (key, value) if key.startsWith("spark.sql") => setConf(key, value)
    case _ =>
  }

  @transient
  protected[sql] val cacheManager = new CacheManager(this)

  /**
   * :: Experimental ::
   * A collection of methods that are considered experimental, but can be used to hook into
   * the query planner for advanced functionality.
   *
   * @group basic
   */
  @Experimental
  @transient
  val experimental: ExperimentalMethods = new ExperimentalMethods(this)

  /**
   * :: Experimental ::
   * Returns a [[DataFrame]] with no rows or columns.
   *
   * @group basic
   */
  @Experimental
  @transient
  lazy val emptyDataFrame: DataFrame = createDataFrame(sparkContext.emptyRDD[Row], StructType(Nil))

  /**
   * A collection of methods for registering user-defined functions (UDF).
   *
   * The following example registers a Scala closure as UDF:
   * {{{
   *   sqlContext.udf.register("myUdf", (arg1: Int, arg2: String) => arg2 + arg1)
   * }}}
   *
   * The following example registers a UDF in Java:
   * {{{
   *   sqlContext.udf().register("myUDF",
   *       new UDF2<Integer, String, String>() {
   *           @Override
   *           public String call(Integer arg1, String arg2) {
   *               return arg2 + arg1;
   *           }
   *      }, DataTypes.StringType);
   * }}}
   *
   * Or, to use Java 8 lambda syntax:
   * {{{
   *   sqlContext.udf().register("myUDF",
   *       (Integer arg1, String arg2) -> arg2 + arg1),
   *       DataTypes.StringType);
   * }}}
   *
   * @group basic
   * TODO move to SQLSession?
   */
  @transient
  val udf: UDFRegistration = new UDFRegistration(this)

  /**
   * Returns true if the table is currently cached in-memory.
   * @group cachemgmt
   */
  def isCached(tableName: String): Boolean = cacheManager.isCached(tableName)

  /**
   * Caches the specified table in-memory.
   * @group cachemgmt
   */
  def cacheTable(tableName: String): Unit = cacheManager.cacheTable(tableName)

  /**
   * Removes the specified table from the in-memory cache.
   * @group cachemgmt
   */
  def uncacheTable(tableName: String): Unit = cacheManager.uncacheTable(tableName)

  /**
   * Removes all cached tables from the in-memory cache.
   */
  def clearCache(): Unit = cacheManager.clearCache()

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  /**
   * :: Experimental ::
   * (Scala-specific) Implicit methods available in Scala for converting
   * common Scala objects into [[DataFrame]]s.
   *
   * {{{
   *   val sqlContext = new SQLContext(sc)
   *   import sqlContext.implicits._
   * }}}
   *
   * @group basic
   */
  @Experimental
  object implicits extends Serializable {
    // scalastyle:on

    /** Converts $"col name" into an [[Column]]. */
    implicit class StringToColumn(val sc: StringContext) {
      def $(args: Any*): ColumnName = {
        new ColumnName(sc.s(args :_*))
      }
    }

    /** An implicit conversion that turns a Scala `Symbol` into a [[Column]]. */
    implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name)

    /** Creates a DataFrame from an RDD of case classes or tuples. */
    implicit def rddToDataFrameHolder[A <: Product : TypeTag](rdd: RDD[A]): DataFrameHolder = {
      DataFrameHolder(self.createDataFrame(rdd))
    }

    /** Creates a DataFrame from a local Seq of Product. */
    implicit def localSeqToDataFrameHolder[A <: Product : TypeTag](data: Seq[A]): DataFrameHolder =
    {
      DataFrameHolder(self.createDataFrame(data))
    }

    // Do NOT add more implicit conversions. They are likely to break source compatibility by
    // making existing implicit conversions ambiguous. In particular, RDD[Double] is dangerous
    // because of [[DoubleRDDFunctions]].

    /** Creates a single column DataFrame from an RDD[Int]. */
    implicit def intRddToDataFrameHolder(data: RDD[Int]): DataFrameHolder = {
      val dataType = IntegerType
      val rows = data.mapPartitions { iter =>
        val row = new SpecificMutableRow(dataType :: Nil)
        iter.map { v =>
          row.setInt(0, v)
          row: Row
        }
      }
      DataFrameHolder(self.createDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
    }

    /** Creates a single column DataFrame from an RDD[Long]. */
    implicit def longRddToDataFrameHolder(data: RDD[Long]): DataFrameHolder = {
      val dataType = LongType
      val rows = data.mapPartitions { iter =>
        val row = new SpecificMutableRow(dataType :: Nil)
        iter.map { v =>
          row.setLong(0, v)
          row: Row
        }
      }
      DataFrameHolder(self.createDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
    }

    /** Creates a single column DataFrame from an RDD[String]. */
    implicit def stringRddToDataFrameHolder(data: RDD[String]): DataFrameHolder = {
      val dataType = StringType
      val rows = data.mapPartitions { iter =>
        val row = new SpecificMutableRow(dataType :: Nil)
        iter.map { v =>
          row.setString(0, v)
          row: Row
        }
      }
      DataFrameHolder(self.createDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
    }
  }

  /**
   * :: Experimental ::
   * Creates a DataFrame from an RDD of case classes.
   *
   * @group dataframes
   */
  @Experimental
  def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame = {
    SparkPlan.currentContext.set(self)
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    val rowRDD = RDDConversions.productToRowRdd(rdd, schema)
    DataFrame(self, LogicalRDD(attributeSeq, rowRDD)(self))
  }

  /**
   * :: Experimental ::
   * Creates a DataFrame from a local Seq of Product.
   *
   * @group dataframes
   */
  @Experimental
  def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame = {
    SparkPlan.currentContext.set(self)
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    DataFrame(self, LocalRelation.fromProduct(attributeSeq, data))
  }

  /**
   * Convert a [[BaseRelation]] created for external data sources into a [[DataFrame]].
   *
   * @group dataframes
   */
  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
    DataFrame(this, LogicalRelation(baseRelation))
  }

  /**
   * :: DeveloperApi ::
   * Creates a [[DataFrame]] from an [[RDD]] containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   * Example:
   * {{{
   *  import org.apache.spark.sql._
   *  import org.apache.spark.sql.types._
   *  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   *
   *  val schema =
   *    StructType(
   *      StructField("name", StringType, false) ::
   *      StructField("age", IntegerType, true) :: Nil)
   *
   *  val people =
   *    sc.textFile("examples/src/main/resources/people.txt").map(
   *      _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
   *  val dataFrame = sqlContext.createDataFrame(people, schema)
   *  dataFrame.printSchema
   *  // root
   *  // |-- name: string (nullable = false)
   *  // |-- age: integer (nullable = true)
   *
   *  dataFrame.registerTempTable("people")
   *  sqlContext.sql("select name from people").collect.foreach(println)
   * }}}
   *
   * @group dataframes
   */
  @DeveloperApi
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD, schema, needsConversion = true)
  }

  /**
   * Creates a DataFrame from an RDD[Row]. User can specify whether the input rows should be
   * converted to Catalyst rows.
   */
  private[sql]
  def createDataFrame(rowRDD: RDD[Row], schema: StructType, needsConversion: Boolean) = {
    // TODO: use MutableProjection when rowRDD is another DataFrame and the applied
    // schema differs from the existing schema on any field data type.
    val catalystRows = if (needsConversion) {
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      rowRDD.map(converter(_).asInstanceOf[Row])
    } else {
      rowRDD
    }
    val logicalPlan = LogicalRDD(schema.toAttributes, catalystRows)(self)
    DataFrame(this, logicalPlan)
  }

  /**
   * :: DeveloperApi ::
   * Creates a [[DataFrame]] from an [[JavaRDD]] containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   *
   * @group dataframes
   */
  @DeveloperApi
  def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD.rdd, schema)
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   * @group dataframes
   */
  def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame = {
    val attributeSeq = getSchema(beanClass)
    val className = beanClass.getName
    val rowRdd = rdd.mapPartitions { iter =>
      // BeanInfo is not serializable so we must rediscover it remotely for each partition.
      val localBeanInfo = Introspector.getBeanInfo(
        Class.forName(className, true, Utils.getContextOrSparkClassLoader))
      val extractors =
        localBeanInfo.getPropertyDescriptors.filterNot(_.getName == "class").map(_.getReadMethod)

      iter.map { row =>
        new GenericRow(
          extractors.zip(attributeSeq).map { case (e, attr) =>
            CatalystTypeConverters.convertToCatalyst(e.invoke(row), attr.dataType)
          }.toArray[Any]
        ) : Row
      }
    }
    DataFrame(this, LogicalRDD(attributeSeq, rowRdd)(this))
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   * @group dataframes
   */
  def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd.rdd, beanClass)
  }

  /**
   * :: DeveloperApi ::
   * Creates a [[DataFrame]] from an [[RDD]] containing [[Row]]s by applying a schema to this RDD.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   * Example:
   * {{{
   *  import org.apache.spark.sql._
   *  import org.apache.spark.sql.types._
   *  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   *
   *  val schema =
   *    StructType(
   *      StructField("name", StringType, false) ::
   *      StructField("age", IntegerType, true) :: Nil)
   *
   *  val people =
   *    sc.textFile("examples/src/main/resources/people.txt").map(
   *      _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
   *  val dataFrame = sqlContext. applySchema(people, schema)
   *  dataFrame.printSchema
   *  // root
   *  // |-- name: string (nullable = false)
   *  // |-- age: integer (nullable = true)
   *
   *  dataFrame.registerTempTable("people")
   *  sqlContext.sql("select name from people").collect.foreach(println)
   * }}}
   */
  @deprecated("use createDataFrame", "1.3.0")
  def applySchema(rowRDD: RDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD, schema)
  }

  @deprecated("use createDataFrame", "1.3.0")
  def applySchema(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD, schema)
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   */
  @deprecated("use createDataFrame", "1.3.0")
  def applySchema(rdd: RDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd, beanClass)
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   */
  @deprecated("use createDataFrame", "1.3.0")
  def applySchema(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd, beanClass)
  }

  /**
   * Loads a Parquet file, returning the result as a [[DataFrame]]. This function returns an empty
   * [[DataFrame]] if no paths are passed in.
   *
   * @group specificdata
   */
  @scala.annotation.varargs
  def parquetFile(paths: String*): DataFrame = {
    if (paths.isEmpty) {
      emptyDataFrame
    } else if (conf.parquetUseDataSourceApi) {
      baseRelationToDataFrame(parquet.ParquetRelation2(paths, Map.empty)(this))
    } else {
      DataFrame(this, parquet.ParquetRelation(
        paths.mkString(","), Some(sparkContext.hadoopConfiguration), this))
    }
  }

  /**
   * Loads a JSON file (one object per line), returning the result as a [[DataFrame]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group specificdata
   */
  def jsonFile(path: String): DataFrame = jsonFile(path, 1.0)

  /**
   * :: Experimental ::
   * Loads a JSON file (one object per line) and applies the given schema,
   * returning the result as a [[DataFrame]].
   *
   * @group specificdata
   */
  @Experimental
  def jsonFile(path: String, schema: StructType): DataFrame =
    load("json", schema, Map("path" -> path))

  /**
   * :: Experimental ::
   * @group specificdata
   */
  @Experimental
  def jsonFile(path: String, samplingRatio: Double): DataFrame =
    load("json", Map("path" -> path, "samplingRatio" -> samplingRatio.toString))

  /**
   * Loads an RDD[String] storing JSON objects (one object per record), returning the result as a
   * [[DataFrame]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group specificdata
   */
  def jsonRDD(json: RDD[String]): DataFrame = jsonRDD(json, 1.0)


  /**
   * Loads an RDD[String] storing JSON objects (one object per record), returning the result as a
   * [[DataFrame]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group specificdata
   */
  def jsonRDD(json: JavaRDD[String]): DataFrame = jsonRDD(json.rdd, 1.0)

  /**
   * :: Experimental ::
   * Loads an RDD[String] storing JSON objects (one object per record) and applies the given schema,
   * returning the result as a [[DataFrame]].
   *
   * @group specificdata
   */
  @Experimental
  def jsonRDD(json: RDD[String], schema: StructType): DataFrame = {
    val columnNameOfCorruptJsonRecord = conf.columnNameOfCorruptRecord
    val appliedSchema =
      Option(schema).getOrElse(
        JsonRDD.nullTypeToStringType(
          JsonRDD.inferSchema(json, 1.0, columnNameOfCorruptJsonRecord)))
    val rowRDD = JsonRDD.jsonStringToRow(json, appliedSchema, columnNameOfCorruptJsonRecord)
    createDataFrame(rowRDD, appliedSchema, needsConversion = false)
  }

  /**
   * :: Experimental ::
   * Loads an JavaRDD<String> storing JSON objects (one object per record) and applies the given
   * schema, returning the result as a [[DataFrame]].
   *
   * @group specificdata
   */
  @Experimental
  def jsonRDD(json: JavaRDD[String], schema: StructType): DataFrame = {
    jsonRDD(json.rdd, schema)
  }

  /**
   * :: Experimental ::
   * Loads an RDD[String] storing JSON objects (one object per record) inferring the
   * schema, returning the result as a [[DataFrame]].
   *
   * @group specificdata
   */
  @Experimental
  def jsonRDD(json: RDD[String], samplingRatio: Double): DataFrame = {
    val columnNameOfCorruptJsonRecord = conf.columnNameOfCorruptRecord
    val appliedSchema =
      JsonRDD.nullTypeToStringType(
        JsonRDD.inferSchema(json, samplingRatio, columnNameOfCorruptJsonRecord))
    val rowRDD = JsonRDD.jsonStringToRow(json, appliedSchema, columnNameOfCorruptJsonRecord)
    createDataFrame(rowRDD, appliedSchema, needsConversion = false)
  }

  /**
   * :: Experimental ::
   * Loads a JavaRDD[String] storing JSON objects (one object per record) inferring the
   * schema, returning the result as a [[DataFrame]].
   *
   * @group specificdata
   */
  @Experimental
  def jsonRDD(json: JavaRDD[String], samplingRatio: Double): DataFrame = {
    jsonRDD(json.rdd, samplingRatio);
  }

  /**
   * :: Experimental ::
   * Returns the dataset stored at path as a DataFrame,
   * using the default data source configured by spark.sql.sources.default.
   *
   * @group genericdata
   */
  @Experimental
  def load(path: String): DataFrame = {
    val dataSourceName = conf.defaultDataSourceName
    load(path, dataSourceName)
  }

  /**
   * :: Experimental ::
   * Returns the dataset stored at path as a DataFrame, using the given data source.
   *
   * @group genericdata
   */
  @Experimental
  def load(path: String, source: String): DataFrame = {
    load(source, Map("path" -> path))
  }

  /**
   * :: Experimental ::
   * (Java-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame.
   *
   * @group genericdata
   */
  @Experimental
  def load(source: String, options: java.util.Map[String, String]): DataFrame = {
    load(source, options.toMap)
  }

  /**
   * :: Experimental ::
   * (Scala-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame.
   *
   * @group genericdata
   */
  @Experimental
  def load(source: String, options: Map[String, String]): DataFrame = {
    val resolved = ResolvedDataSource(this, None, source, options)
    DataFrame(this, LogicalRelation(resolved.relation))
  }

  /**
   * :: Experimental ::
   * (Java-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame, using the given schema as the schema of the DataFrame.
   *
   * @group genericdata
   */
  @Experimental
  def load(
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]): DataFrame = {
    load(source, schema, options.toMap)
  }

  /**
   * :: Experimental ::
   * (Scala-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame, using the given schema as the schema of the DataFrame.
   * @group genericdata
   */
  @Experimental
  def load(
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    val resolved = ResolvedDataSource(this, Some(schema), source, options)
    DataFrame(this, LogicalRelation(resolved.relation))
  }

  /**
   * :: Experimental ::
   * Creates an external table from the given path and returns the corresponding DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
   *
   * @group ddl_ops
   */
  @Experimental
  def createExternalTable(tableName: String, path: String): DataFrame = {
    val dataSourceName = conf.defaultDataSourceName
    createExternalTable(tableName, path, dataSourceName)
  }

  /**
   * :: Experimental ::
   * Creates an external table from the given path based on a data source
   * and returns the corresponding DataFrame.
   *
   * @group ddl_ops
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      path: String,
      source: String): DataFrame = {
    createExternalTable(tableName, source, Map("path" -> path))
  }

  /**
   * :: Experimental ::
   * Creates an external table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      options: java.util.Map[String, String]): DataFrame = {
    createExternalTable(tableName, source, options.toMap)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Creates an external table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame = {
    val cmd =
      CreateTableUsing(
        tableName,
        userSpecifiedSchema = None,
        source,
        temporary = false,
        options,
        allowExisting = false,
        managedIfNoPath = false)
    executePlan(cmd).toRdd
    table(tableName)
  }

  /**
   * :: Experimental ::
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]): DataFrame = {
    createExternalTable(tableName, source, schema, options.toMap)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    val cmd =
      CreateTableUsing(
        tableName,
        userSpecifiedSchema = Some(schema),
        source,
        temporary = false,
        options,
        allowExisting = false,
        managedIfNoPath = false)
    executePlan(cmd).toRdd
    table(tableName)
  }

  /**
   * :: Experimental ::
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table.
   *
   * @group specificdata
   */
  @Experimental
  def jdbc(url: String, table: String): DataFrame = {
    jdbc(url, table, JDBCRelation.columnPartition(null))
  }

  /**
   * :: Experimental ::
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table.  Partitions of the table will be retrieved in parallel based on the parameters
   * passed to this function.
   *
   * @param columnName the name of a column of integral type that will be used for partitioning.
   * @param lowerBound the minimum value of `columnName` used to decide partition stride
   * @param upperBound the maximum value of `columnName` used to decide partition stride
   * @param numPartitions the number of partitions.  the range `minValue`-`maxValue` will be split
   *                      evenly into this many partitions
   *
   * @group specificdata
   */
  @Experimental
  def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int): DataFrame = {
    val partitioning = JDBCPartitioningInfo(columnName, lowerBound, upperBound, numPartitions)
    val parts = JDBCRelation.columnPartition(partitioning)
    jdbc(url, table, parts)
  }

  /**
   * :: Experimental ::
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table.  The theParts parameter gives a list expressions
   * suitable for inclusion in WHERE clauses; each one defines one partition
   * of the [[DataFrame]].
   *
   * @group specificdata
   */
  @Experimental
  def jdbc(url: String, table: String, theParts: Array[String]): DataFrame = {
    val parts: Array[Partition] = theParts.zipWithIndex.map { case (part, i) =>
      JDBCPartition(part, i) : Partition
    }
    jdbc(url, table, parts)
  }

  private def jdbc(url: String, table: String, parts: Array[Partition]): DataFrame = {
    val relation = JDBCRelation(url, table, parts)(this)
    baseRelationToDataFrame(relation)
  }

  /**
   * Registers the given [[DataFrame]] as a temporary table in the catalog. Temporary tables exist
   * only during the lifetime of this instance of SQLContext.
   */
  private[sql] def registerDataFrameAsTable(df: DataFrame, tableName: String): Unit = {
    catalog.registerTable(Seq(tableName), df.logicalPlan)
  }

  /**
   * Drops the temporary table with the given table name in the catalog. If the table has been
   * cached/persisted before, it's also unpersisted.
   *
   * @param tableName the name of the table to be unregistered.
   *
   * @group basic
   */
  def dropTempTable(tableName: String): Unit = {
    cacheManager.tryUncacheQuery(table(tableName))
    catalog.unregisterTable(Seq(tableName))
  }

  /**
   * Executes a SQL query using Spark, returning the result as a [[DataFrame]]. The dialect that is
   * used for SQL parsing can be configured with 'spark.sql.dialect'.
   *
   * @group basic
   */
  def sql(sqlText: String): DataFrame = {
    DataFrame(this, parseSql(sqlText))
  }

  /**
   * Returns the specified table as a [[DataFrame]].
   *
   * @group ddl_ops
   */
  def table(tableName: String): DataFrame =
    DataFrame(this, catalog.lookupRelation(Seq(tableName)))

  /**
   * Returns a [[DataFrame]] containing names of existing tables in the current database.
   * The returned DataFrame has two columns, tableName and isTemporary (a Boolean
   * indicating if a table is a temporary one or not).
   *
   * @group ddl_ops
   */
  def tables(): DataFrame = {
    DataFrame(this, ShowTablesCommand(None))
  }

  /**
   * Returns a [[DataFrame]] containing names of existing tables in the given database.
   * The returned DataFrame has two columns, tableName and isTemporary (a Boolean
   * indicating if a table is a temporary one or not).
   *
   * @group ddl_ops
   */
  def tables(databaseName: String): DataFrame = {
    DataFrame(this, ShowTablesCommand(Some(databaseName)))
  }

  /**
   * Returns the names of tables in the current database as an array.
   *
   * @group ddl_ops
   */
  def tableNames(): Array[String] = {
    catalog.getTables(None).map {
      case (tableName, _) => tableName
    }.toArray
  }

  /**
   * Returns the names of tables in the given database as an array.
   *
   * @group ddl_ops
   */
  def tableNames(databaseName: String): Array[String] = {
    catalog.getTables(Some(databaseName)).map {
      case (tableName, _) => tableName
    }.toArray
  }

  protected[sql] class SparkPlanner extends SparkStrategies {
    val sparkContext: SparkContext = self.sparkContext

    val sqlContext: SQLContext = self

    def codegenEnabled: Boolean = self.conf.codegenEnabled

    def unsafeEnabled: Boolean = self.conf.unsafeEnabled

    def numPartitions: Int = self.conf.numShufflePartitions

    def strategies: Seq[Strategy] =
      experimental.extraStrategies ++ (
      DataSourceStrategy ::
      DDLStrategy ::
      TakeOrdered ::
      HashAggregation ::
      LeftSemiJoin ::
      HashJoin ::
      InMemoryScans ::
      ParquetOperations ::
      BasicOperators ::
      CartesianProduct ::
      BroadcastNestedLoopJoin :: Nil)

    /**
     * Used to build table scan operators where complex projection and filtering are done using
     * separate physical operators.  This function returns the given scan operator with Project and
     * Filter nodes added only when needed.  For example, a Project operator is only used when the
     * final desired output requires complex expressions to be evaluated or when columns can be
     * further eliminated out after filtering has been done.
     *
     * The `prunePushedDownFilters` parameter is used to remove those filters that can be optimized
     * away by the filter pushdown optimization.
     *
     * The required attributes for both filtering and expression evaluation are passed to the
     * provided `scanBuilder` function so that it can avoid unnecessary column materialization.
     */
    def pruneFilterProject(
        projectList: Seq[NamedExpression],
        filterPredicates: Seq[Expression],
        prunePushedDownFilters: Seq[Expression] => Seq[Expression],
        scanBuilder: Seq[Attribute] => SparkPlan): SparkPlan = {

      val projectSet = AttributeSet(projectList.flatMap(_.references))
      val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
      val filterCondition =
        prunePushedDownFilters(filterPredicates).reduceLeftOption(expressions.And)

      // Right now we still use a projection even if the only evaluation is applying an alias
      // to a column.  Since this is a no-op, it could be avoided. However, using this
      // optimization with the current implementation would change the output schema.
      // TODO: Decouple final output schema from expression evaluation so this copy can be
      // avoided safely.

      if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
          filterSet.subsetOf(projectSet)) {
        // When it is possible to just use column pruning to get the right projection and
        // when the columns of this projection are enough to evaluate all filter conditions,
        // just do a scan followed by a filter, with no extra project.
        val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
        filterCondition.map(Filter(_, scan)).getOrElse(scan)
      } else {
        val scan = scanBuilder((projectSet ++ filterSet).toSeq)
        Project(projectList, filterCondition.map(Filter(_, scan)).getOrElse(scan))
      }
    }
  }

  @transient
  protected[sql] val planner = new SparkPlanner

  @transient
  protected[sql] lazy val emptyResult = sparkContext.parallelize(Seq.empty[Row], 1)

  /**
   * Prepares a planned SparkPlan for execution by inserting shuffle operations as needed.
   */
  @transient
  protected[sql] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches =
      Batch("Add exchange", Once, EnsureRequirements(self)) :: Nil
  }

  protected[sql] def openSession(): SQLSession = {
    detachSession()
    val session = createSession()
    tlSession.set(session)

    session
  }

  protected[sql] def currentSession(): SQLSession = {
    tlSession.get()
  }

  protected[sql] def createSession(): SQLSession = {
    new this.SQLSession()
  }

  protected[sql] def detachSession(): Unit = {
    tlSession.remove()
  }

  protected[sql] class SQLSession {
    // Note that this is a lazy val so we can override the default value in subclasses.
    protected[sql] lazy val conf: SQLConf = new SQLConf
  }

  /**
   * :: DeveloperApi ::
   * The primary workflow for executing relational queries using Spark.  Designed to allow easy
   * access to the intermediate phases of query execution for developers.
   */
  @DeveloperApi
  protected[sql] class QueryExecution(val logical: LogicalPlan) {
    def assertAnalyzed(): Unit = analyzer.checkAnalysis(analyzed)

    lazy val analyzed: LogicalPlan = analyzer.execute(logical)
    lazy val withCachedData: LogicalPlan = {
      assertAnalyzed()
      cacheManager.useCachedData(analyzed)
    }
    lazy val optimizedPlan: LogicalPlan = optimizer.execute(withCachedData)

    // TODO: Don't just pick the first one...
    lazy val sparkPlan: SparkPlan = {
      SparkPlan.currentContext.set(self)
      planner(optimizedPlan).next()
    }
    // executedPlan should not be used to initialize any SparkPlan. It should be
    // only used for execution.
    lazy val executedPlan: SparkPlan = prepareForExecution.execute(sparkPlan)

    /** Internal version of the RDD. Avoids copies and has no schema */
    lazy val toRdd: RDD[Row] = executedPlan.execute()

    protected def stringOrError[A](f: => A): String =
      try f.toString catch { case e: Throwable => e.toString }

    def simpleString: String =
      s"""== Physical Plan ==
         |${stringOrError(executedPlan)}
      """.stripMargin.trim

    override def toString: String =
      // TODO previously will output RDD details by run (${stringOrError(toRdd.toDebugString)})
      // however, the `toRdd` will cause the real execution, which is not what we want.
      // We need to think about how to avoid the side effect.
      s"""== Parsed Logical Plan ==
         |${stringOrError(logical)}
         |== Analyzed Logical Plan ==
         |${stringOrError(analyzed)}
         |== Optimized Logical Plan ==
         |${stringOrError(optimizedPlan)}
         |== Physical Plan ==
         |${stringOrError(executedPlan)}
         |Code Generation: ${stringOrError(executedPlan.codegenEnabled)}
         |== RDD ==
      """.stripMargin.trim
  }

  /**
   * Parses the data type in our internal string representation. The data type string should
   * have the same format as the one generated by `toString` in scala.
   * It is only used by PySpark.
   */
  protected[sql] def parseDataType(dataTypeString: String): DataType = {
    DataType.fromJson(dataTypeString)
  }

  /**
   * Apply a schema defined by the schemaString to an RDD. It is only used by PySpark.
   */
  protected[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schemaString: String): DataFrame = {
    val schema = parseDataType(schemaString).asInstanceOf[StructType]
    applySchemaToPythonRDD(rdd, schema)
  }

  /**
   * Apply a schema defined by the schema to an RDD. It is only used by PySpark.
   */
  protected[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schema: StructType): DataFrame = {

    def needsConversion(dataType: DataType): Boolean = dataType match {
      case ByteType => true
      case ShortType => true
      case LongType => true
      case FloatType => true
      case DateType => true
      case TimestampType => true
      case StringType => true
      case ArrayType(_, _) => true
      case MapType(_, _, _) => true
      case StructType(_) => true
      case udt: UserDefinedType[_] => needsConversion(udt.sqlType)
      case other => false
    }

    val convertedRdd = if (schema.fields.exists(f => needsConversion(f.dataType))) {
      rdd.map(m => m.zip(schema.fields).map {
        case (value, field) => EvaluatePython.fromJava(value, field.dataType)
      })
    } else {
      rdd
    }

    val rowRdd = convertedRdd.mapPartitions { iter =>
      iter.map { m => new GenericRow(m): Row}
    }

    DataFrame(this, LogicalRDD(schema.toAttributes, rowRdd)(self))
  }

  /**
   * Returns a Catalyst Schema for the given java bean class.
   */
  protected def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
    val (dataType, _) = JavaTypeInference.inferDataType(TypeToken.of(beanClass))
    dataType.asInstanceOf[StructType].fields.map { f =>
      AttributeReference(f.name, f.dataType, f.nullable)()
    }
  }

}


