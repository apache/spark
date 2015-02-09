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

package org.apache.spark.sql.hive.api.java

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.api.java.{JavaSQLContext, JavaSchemaRDD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.{HiveContext, HiveQl}

/**
 * The entry point for executing Spark SQL queries from a Java program.
 */
class JavaHiveContext(sqlContext: SQLContext) extends JavaSQLContext(sqlContext) {

  def this(sparkContext: JavaSparkContext) = this(new HiveContext(sparkContext))

  override def sql(sqlText: String): JavaSchemaRDD = {
    // TODO: Create a framework for registering parsers instead of just hardcoding if statements.
    if (sqlContext.dialect == "sql") {
      super.sql(sqlText)
    } else if (sqlContext.dialect == "hiveql") {
      new JavaSchemaRDD(sqlContext, HiveQl.parseSql(sqlText))
    }  else {
      sys.error(s"Unsupported SQL dialect: ${sqlContext.dialect}.  Try 'sql' or 'hiveql'")
    }
  }

  /**
    * DEPRECATED: Use sql(...) Instead
    */
  @Deprecated
  def hql(hqlQuery: String): JavaSchemaRDD =
    new JavaSchemaRDD(sqlContext, HiveQl.parseSql(hqlQuery))
}
