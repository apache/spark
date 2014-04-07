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

import java.beans.{Introspector, PropertyDescriptor}

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericRow, Row => ScalaRow}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.parquet.ParquetRelation
import org.apache.spark.sql.execution.{ExistingRdd, SparkLogicalPlan}

/**
 * The entry point for executing Spark SQL queries from a Java program.
 */
class JavaSQLContext(sparkContext: JavaSparkContext) {

  val sqlContext = new SQLContext(sparkContext.sc)

  /**
   * Executes a query expressed in SQL, returning the result as a JavaSchemaRDD
   */
  def sql(sqlQuery: String): JavaSchemaRDD = {
    val result = new JavaSchemaRDD(sqlContext, sqlContext.parseSql(sqlQuery))
    // We force query optimization to happen right away instead of letting it happen lazily like
    // when using the query DSL.  This is so DDL commands behave as expected.  This is only
    // generates the RDD lineage for DML queries, but do not perform any execution.
    result.queryExecution.toRdd
    result
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   */
  def applySchema(rdd: JavaRDD[_], beanClass: Class[_]): JavaSchemaRDD = {
    // TODO: All of this could probably be moved to Catalyst as it is mostly not Spark specific.
    val beanInfo = Introspector.getBeanInfo(beanClass)

    val fields = beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
    val schema = fields.map { property =>
      val dataType = property.getPropertyType match {
        case c: Class[_] if c == classOf[java.lang.String] => StringType
        case c: Class[_] if c == java.lang.Short.TYPE => ShortType
        case c: Class[_] if c == java.lang.Integer.TYPE => IntegerType
        case c: Class[_] if c == java.lang.Long.TYPE => LongType
        case c: Class[_] if c == java.lang.Double.TYPE => DoubleType
        case c: Class[_] if c == java.lang.Byte.TYPE => ByteType
        case c: Class[_] if c == java.lang.Float.TYPE => FloatType
        case c: Class[_] if c == java.lang.Boolean.TYPE => BooleanType
      }

      AttributeReference(property.getName, dataType, true)()
    }

    val className = beanClass.getCanonicalName
    val rowRdd = rdd.rdd.mapPartitions { iter =>
      // BeanInfo is not serializable so we must rediscover it remotely for each partition.
      val localBeanInfo = Introspector.getBeanInfo(Class.forName(className))
      val extractors =
        localBeanInfo.getPropertyDescriptors.filterNot(_.getName == "class").map(_.getReadMethod)

      iter.map { row =>
        new GenericRow(extractors.map(e => e.invoke(row)).toArray[Any]): ScalaRow
      }
    }
    new JavaSchemaRDD(sqlContext, SparkLogicalPlan(ExistingRdd(schema, rowRdd)))
  }

  /**
   * Applies a schema to an RDD of Array[Any]
   */
  def applySchema(rdd: JavaRDD[_]): JavaSchemaRDD = {
    val fields = rdd.first match {
      case row: java.util.ArrayList[_] => row.toArray.map(_.getClass)
      case row => throw new Exception(s"Rows must be Lists 1 ${row.getClass}")
    }

    val schema = fields.zipWithIndex.map { case (klass, index) =>
      val dataType = klass match {
        case c: Class[_] if c == classOf[java.lang.String] => StringType
        case c: Class[_] if c == classOf[java.lang.Integer] => IntegerType
       // case c: Class[_] if c == java.lang.Short.TYPE => ShortType
       // case c: Class[_] if c == java.lang.Integer.TYPE => IntegerType
       // case c: Class[_] if c == java.lang.Long.TYPE => LongType
       // case c: Class[_] if c == java.lang.Double.TYPE => DoubleType
       // case c: Class[_] if c == java.lang.Byte.TYPE => ByteType
       // case c: Class[_] if c == java.lang.Float.TYPE => FloatType
       // case c: Class[_] if c == java.lang.Boolean.TYPE => BooleanType
      }

      AttributeReference(index.toString, dataType, true)()
    }

    val rowRdd = rdd.rdd.mapPartitions { iter =>
      iter.map {
        case row: java.util.ArrayList[_] => new GenericRow(row.toArray.asInstanceOf[Array[Any]]): ScalaRow
        case row => throw new Exception(s"Rows must be Lists 2 ${row.getClass}")
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
}
