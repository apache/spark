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

import org.apache.spark.api.java.JavaSparkContext

import org.apache.spark.sql._

class JavaSQLContext(sparkContext: JavaSparkContext) {

  val sqlContext = new SQLContext(sparkContext)

  def sql(sqlQuery: String): JavaSchemaRDD = {
    val result = new JavaSchemaRDD(sqlContext, sqlContext.parseSql(sqlQuery))
    // We force query optimization to happen right away instead of letting it happen lazily like
    // when using the query DSL.  This is so DDL commands behave as expected.  This is only
    // generates the RDD lineage for DML queries, but do not perform any execution.
    result.queryExecution.toRdd
    result
  }

  /**
   * Loads a parquet file, returning the result as a [[SchemaRDD]].
   */
  def parquetFile(path: String): JavaSchemaRDD =
    new JavaSchemaRDD(sqlContext, parquet.ParquetRelation("ParquetFile", path))


  /**
   * Registers the given RDD as a temporary table in the catalog.  Temporary tables exist only
   * during the lifetime of this instance of SQLContext.
   */
  def registerRDDAsTable(rdd: JavaSchemaRDD, tableName: String): Unit = {
    sqlContext.registerRDDAsTable(rdd.baseSchemaRDD, tableName)
  }
}
