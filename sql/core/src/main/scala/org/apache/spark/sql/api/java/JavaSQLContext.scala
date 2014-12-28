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

package org.apache.spark.sql.api.java

import java.beans.Introspector

import org.apache.hadoop.conf.Configuration

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.{SQLContext, StructType => SStructType}
import org.apache.spark.sql.catalyst.annotation.SQLUserDefinedType
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericRow, Row => ScalaRow}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.json.JsonRDD
import org.apache.spark.sql.parquet.ParquetRelation
import org.apache.spark.sql.sources.{LogicalRelation, BaseRelation}
import org.apache.spark.sql.types.util.DataTypeConversions
import org.apache.spark.sql.types.util.DataTypeConversions.asScalaDataType
import org.apache.spark.util.Utils

/**
 * The entry point for executing Spark SQL queries from a Java program.
 */
class JavaSQLContext(val sqlContext: SQLContext) extends UDFRegistration {

  def this(sparkContext: JavaSparkContext) = this(new SQLContext(sparkContext.sc))

  def baseRelationToSchemaRDD(baseRelation: BaseRelation): JavaSchemaRDD = {
    new JavaSchemaRDD(sqlContext, LogicalRelation(baseRelation))
  }

  /**
   * Executes a SQL query using Spark, returning the result as a SchemaRDD.  The dialect that is
   * used for SQL parsing can be configured with 'spark.sql.dialect'.
   *
   * @group userf
   */
  def sql(sqlText: String): JavaSchemaRDD = {
    if (sqlContext.dialect == "sql") {
      new JavaSchemaRDD(sqlContext, sqlContext.parseSql(sqlText))
    } else {
      sys.error(s"Unsupported SQL dialect: $sqlContext.dialect")
    }
  }

  /**
   * :: Experimental ::
   * Creates an empty parquet file with the schema of class `beanClass`, which can be registered as
   * a table. This registered table can be used as the target of future `insertInto` operations.
   *
   * {{{
   *   JavaSQLContext sqlCtx = new JavaSQLContext(...)
   *
   *   sqlCtx.createParquetFile(Person.class, "path/to/file.parquet").registerTempTable("people")
   *   sqlCtx.sql("INSERT INTO people SELECT 'michael', 29")
   * }}}
   *
   * @param beanClass A java bean class object that will be used to determine the schema of the
   *                  parquet file.
   * @param path The path where the directory containing parquet metadata should be created.
   *             Data inserted into this table will also be stored at this location.
   * @param allowExisting When false, an exception will be thrown if this directory already exists.
   * @param conf A Hadoop configuration object that can be used to specific options to the parquet
   *             output format.
   */
  @Experimental
  def createParquetFile(
      beanClass: Class[_],
      path: String,
      allowExisting: Boolean = true,
      conf: Configuration = new Configuration()): JavaSchemaRDD = {
    new JavaSchemaRDD(
      sqlContext,
      ParquetRelation.createEmpty(path, getSchema(beanClass), allowExisting, conf, sqlContext))
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   */
  def applySchema(rdd: JavaRDD[_], beanClass: Class[_]): JavaSchemaRDD = {
    val attributeSeq = getSchema(beanClass)
    val className = beanClass.getName
    val rowRdd = rdd.rdd.mapPartitions { iter =>
      // BeanInfo is not serializable so we must rediscover it remotely for each partition.
      val localBeanInfo = Introspector.getBeanInfo(
        Class.forName(className, true, Utils.getContextOrSparkClassLoader))
      val extractors =
        localBeanInfo.getPropertyDescriptors.filterNot(_.getName == "class").map(_.getReadMethod)

      iter.map { row =>
        new GenericRow(
          extractors.zip(attributeSeq).map { case (e, attr) =>
            DataTypeConversions.convertJavaToCatalyst(e.invoke(row), attr.dataType)
          }.toArray[Any]
        ): ScalaRow
      }
    }
    new JavaSchemaRDD(sqlContext, LogicalRDD(attributeSeq, rowRdd)(sqlContext))
  }

  /**
   * :: DeveloperApi ::
   * Creates a JavaSchemaRDD from an RDD containing Rows by applying a schema to this RDD.
   * It is important to make sure that the structure of every Row of the provided RDD matches the
   * provided schema. Otherwise, there will be runtime exception.
   */
  @DeveloperApi
  def applySchema(rowRDD: JavaRDD[Row], schema: StructType): JavaSchemaRDD = {
    val scalaRowRDD = rowRDD.rdd.map(r => r.row)
    val scalaSchema = asScalaDataType(schema).asInstanceOf[SStructType]
    val logicalPlan =
      LogicalRDD(scalaSchema.toAttributes, scalaRowRDD)(sqlContext)
    new JavaSchemaRDD(sqlContext, logicalPlan)
  }

  /**
   * Loads a parquet file from regular path or files that match file patterns in path,
   * returning the result as a [[JavaSchemaRDD]].
   * Supported glob file pattern information at ([[http://tinyurl.com/kcqrzn8]]).
   */
  def parquetFile(path: String): JavaSchemaRDD =
    new JavaSchemaRDD(
      sqlContext,
      ParquetRelation(path, Some(sqlContext.sparkContext.hadoopConfiguration), sqlContext))

  /**
   * Loads a JSON file (one object per line), returning the result as a JavaSchemaRDD.
   * It goes through the entire dataset once to determine the schema.
   */
  def jsonFile(path: String): JavaSchemaRDD =
    jsonRDD(sqlContext.sparkContext.textFile(path))

  /**
   * :: Experimental ::
   * Loads a JSON file (one object per line) and applies the given schema,
   * returning the result as a JavaSchemaRDD.
   */
  @Experimental
  def jsonFile(path: String, schema: StructType): JavaSchemaRDD =
    jsonRDD(sqlContext.sparkContext.textFile(path), schema)

  /**
   * Loads an RDD[String] storing JSON objects (one object per record), returning the result as a
   * JavaSchemaRDD.
   * It goes through the entire dataset once to determine the schema.
   */
  def jsonRDD(json: JavaRDD[String]): JavaSchemaRDD = {
    val columnNameOfCorruptJsonRecord = sqlContext.columnNameOfCorruptRecord
    val appliedScalaSchema =
      JsonRDD.nullTypeToStringType(
        JsonRDD.inferSchema(json.rdd, 1.0, columnNameOfCorruptJsonRecord))
    val scalaRowRDD =
      JsonRDD.jsonStringToRow(json.rdd, appliedScalaSchema, columnNameOfCorruptJsonRecord)
    val logicalPlan =
      LogicalRDD(appliedScalaSchema.toAttributes, scalaRowRDD)(sqlContext)
    new JavaSchemaRDD(sqlContext, logicalPlan)
  }

  /**
   * :: Experimental ::
   * Loads an RDD[String] storing JSON objects (one object per record) and applies the given schema,
   * returning the result as a JavaSchemaRDD.
   */
  @Experimental
  def jsonRDD(json: JavaRDD[String], schema: StructType): JavaSchemaRDD = {
    val columnNameOfCorruptJsonRecord = sqlContext.columnNameOfCorruptRecord
    val appliedScalaSchema =
      Option(asScalaDataType(schema)).getOrElse(
        JsonRDD.nullTypeToStringType(
          JsonRDD.inferSchema(
            json.rdd, 1.0, columnNameOfCorruptJsonRecord))).asInstanceOf[SStructType]
    val scalaRowRDD = JsonRDD.jsonStringToRow(
      json.rdd, appliedScalaSchema, columnNameOfCorruptJsonRecord)
    val logicalPlan =
      LogicalRDD(appliedScalaSchema.toAttributes, scalaRowRDD)(sqlContext)
    new JavaSchemaRDD(sqlContext, logicalPlan)
  }

  /**
   * Registers the given RDD as a temporary table in the catalog.  Temporary tables exist only
   * during the lifetime of this instance of SQLContext.
   */
  def registerRDDAsTable(rdd: JavaSchemaRDD, tableName: String): Unit = {
    sqlContext.registerRDDAsTable(rdd.baseSchemaRDD, tableName)
  }

  /**
   * Returns a Catalyst Schema for the given java bean class.
   */
  protected def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
    // TODO: All of this could probably be moved to Catalyst as it is mostly not Spark specific.
    val beanInfo = Introspector.getBeanInfo(beanClass)

    // Note: The ordering of elements may differ from when the schema is inferred in Scala.
    //       This is because beanInfo.getPropertyDescriptors gives no guarantees about
    //       element ordering.
    val fields = beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
    fields.map { property =>
      val (dataType, nullable) = property.getPropertyType match {
        case c: Class[_] if c.isAnnotationPresent(classOf[SQLUserDefinedType]) =>
          (c.getAnnotation(classOf[SQLUserDefinedType]).udt().newInstance(), true)
        case c: Class[_] if c == classOf[java.lang.String] =>
          (org.apache.spark.sql.StringType, true)
        case c: Class[_] if c == java.lang.Short.TYPE =>
          (org.apache.spark.sql.ShortType, false)
        case c: Class[_] if c == java.lang.Integer.TYPE =>
          (org.apache.spark.sql.IntegerType, false)
        case c: Class[_] if c == java.lang.Long.TYPE =>
          (org.apache.spark.sql.LongType, false)
        case c: Class[_] if c == java.lang.Double.TYPE =>
          (org.apache.spark.sql.DoubleType, false)
        case c: Class[_] if c == java.lang.Byte.TYPE =>
          (org.apache.spark.sql.ByteType, false)
        case c: Class[_] if c == java.lang.Float.TYPE =>
          (org.apache.spark.sql.FloatType, false)
        case c: Class[_] if c == java.lang.Boolean.TYPE =>
          (org.apache.spark.sql.BooleanType, false)

        case c: Class[_] if c == classOf[java.lang.Short] =>
          (org.apache.spark.sql.ShortType, true)
        case c: Class[_] if c == classOf[java.lang.Integer] =>
          (org.apache.spark.sql.IntegerType, true)
        case c: Class[_] if c == classOf[java.lang.Long] =>
          (org.apache.spark.sql.LongType, true)
        case c: Class[_] if c == classOf[java.lang.Double] =>
          (org.apache.spark.sql.DoubleType, true)
        case c: Class[_] if c == classOf[java.lang.Byte] =>
          (org.apache.spark.sql.ByteType, true)
        case c: Class[_] if c == classOf[java.lang.Float] =>
          (org.apache.spark.sql.FloatType, true)
        case c: Class[_] if c == classOf[java.lang.Boolean] =>
          (org.apache.spark.sql.BooleanType, true)
        case c: Class[_] if c == classOf[java.math.BigDecimal] =>
          (org.apache.spark.sql.DecimalType(), true)
        case c: Class[_] if c == classOf[java.sql.Date] =>
          (org.apache.spark.sql.DateType, true)
        case c: Class[_] if c == classOf[java.sql.Timestamp] =>
          (org.apache.spark.sql.TimestampType, true)
      }
      AttributeReference(property.getName, dataType, nullable)()
    }
  }
}
