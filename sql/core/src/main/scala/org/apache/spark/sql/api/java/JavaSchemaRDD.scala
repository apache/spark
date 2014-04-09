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

import org.apache.spark.api.java.{JavaRDDLike, JavaRDD}
import org.apache.spark.sql.{SQLContext, SchemaRDD, SchemaRDDLike}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.rdd.RDD

/**
 * An RDD of [[Row]] objects that is returned as the result of a Spark SQL query.  In addition to
 * standard RDD operations, a JavaSchemaRDD can also be registered as a table in the JavaSQLContext
 * that was used to create. Registering a JavaSchemaRDD allows its contents to be queried in
 * future SQL statement.
 *
 * @groupname schema SchemaRDD Functions
 * @groupprio schema -1
 * @groupname Ungrouped Base RDD Functions
 */
class JavaSchemaRDD(
     @transient val sqlContext: SQLContext,
     @transient protected[spark] val logicalPlan: LogicalPlan)
  extends JavaRDDLike[Row, JavaRDD[Row]]
  with SchemaRDDLike {

  private[sql] val baseSchemaRDD = new SchemaRDD(sqlContext, logicalPlan)

  override val classTag = scala.reflect.classTag[Row]

  override def wrapRDD(rdd: RDD[Row]): JavaRDD[Row] = JavaRDD.fromRDD(rdd)

  val rdd = baseSchemaRDD.map(new Row(_))
}
