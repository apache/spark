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

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericRow, Row => ScalaRow}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.parquet.ParquetRelation
import org.apache.spark.sql.execution.{ExistingRdd, SparkLogicalPlan}
import org.apache.spark.util.Utils

/**
 * The entry point for executing Spark SQL queries from a Java program.
 */
class JavaSQLContext(val sqlContext: SQLContext) {

  def this(sparkContext: JavaSparkContext) = this(new SQLContext(sparkContext.sc))

  /**
   * Executes a query expressed in SQL, returning the result as a JavaSchemaRDD
   */
  def sql(sqlQuery: String): JavaSchemaRDD =
    new JavaSchemaRDD(sqlContext, sqlContext.parseSql(sqlQuery))

  /**
   * :: Experimental ::
   * Creates an empty parquet file with the schema of class `beanClass`, which can be registered as
   * a table. This registered table can be used as the target of future `insertInto` operations.
   *
   * {{{
   *   JavaSQLContext sqlCtx = new JavaSQLContext(...)
   *
   *   sqlCtx.createParquetFile(Person.class, "path/to/file.parquet").registerAsTable("people")
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
      ParquetRelation.createEmpty(path, getSchema(beanClass), allowExisting, conf))
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   */
  def applySchema(rdd: JavaRDD[_], beanClass: Class[_]): JavaSchemaRDD = {
    val schema = getSchema(beanClass)
    val className = beanClass.getName
    val rowRdd = rdd.rdd.mapPartitions { iter =>
      // BeanInfo is not serializable so we must rediscover it remotely for each partition.
      val localBeanInfo = Introspector.getBeanInfo(
        Class.forName(className, true, Utils.getContextOrSparkClassLoader))
      val extractors =
        localBeanInfo.getPropertyDescriptors.filterNot(_.getName == "class").map(_.getReadMethod)

      iter.map { row =>
        new GenericRow(extractors.map(e => e.invoke(row)).toArray[Any]): ScalaRow
      }
    }
    new JavaSchemaRDD(sqlContext, SparkLogicalPlan(ExistingRdd(schema, rowRdd)))
  }

  /**
   * Loads a parquet file, returning the result as a [[JavaSchemaRDD]].
   */
  def parquetFile(path: String): JavaSchemaRDD =
    new JavaSchemaRDD(sqlContext, ParquetRelation(path))

  /**
   * Registers the given RDD as a temporary table in the catalog.  Temporary tables exist only
   * during the lifetime of this instance of SQLContext.
   */
  def registerRDDAsTable(rdd: JavaSchemaRDD, tableName: String): Unit = {
    sqlContext.registerRDDAsTable(rdd.baseSchemaRDD, tableName)
  }

  /** Returns a Catalyst Schema for the given java bean class. */
  protected def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
    // TODO: All of this could probably be moved to Catalyst as it is mostly not Spark specific.
    val beanInfo = Introspector.getBeanInfo(beanClass)

    val fields = beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
    fields.map { property =>
      val dataType = property.getPropertyType match {
        case c: Class[_] if c == classOf[java.lang.String] => StringType
        case c: Class[_] if c == java.lang.Short.TYPE => ShortType
        case c: Class[_] if c == java.lang.Integer.TYPE => IntegerType
        case c: Class[_] if c == java.lang.Long.TYPE => LongType
        case c: Class[_] if c == java.lang.Double.TYPE => DoubleType
        case c: Class[_] if c == java.lang.Byte.TYPE => ByteType
        case c: Class[_] if c == java.lang.Float.TYPE => FloatType
        case c: Class[_] if c == java.lang.Boolean.TYPE => BooleanType

        case c: Class[_] if c == classOf[java.lang.Short] => ShortType
        case c: Class[_] if c == classOf[java.lang.Integer] => IntegerType
        case c: Class[_] if c == classOf[java.lang.Long] => LongType
        case c: Class[_] if c == classOf[java.lang.Double] => DoubleType
        case c: Class[_] if c == classOf[java.lang.Byte] => ByteType
        case c: Class[_] if c == classOf[java.lang.Float] => FloatType
        case c: Class[_] if c == classOf[java.lang.Boolean] => BooleanType
      }
      // TODO: Nullability could be stricter.
      AttributeReference(property.getName, dataType, nullable = true)()
    }
  }
}
