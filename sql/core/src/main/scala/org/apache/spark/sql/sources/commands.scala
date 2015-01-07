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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types.{StructType, StructField}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.util.Utils

private[sql] case class CreateTableUsing(
    tableName: String,
    tableCols: Seq[StructField],
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
    val relation = clazz.newInstance match {
      case dataSource: org.apache.spark.sql.sources.RelationProvider  =>
        dataSource
          .asInstanceOf[org.apache.spark.sql.sources.RelationProvider]
          .createRelation(sqlContext, new CaseInsensitiveMap(options))
      case dataSource: org.apache.spark.sql.sources.SchemaRelationProvider =>
        if(tableCols.isEmpty) {
          dataSource
            .asInstanceOf[org.apache.spark.sql.sources.SchemaRelationProvider]
            .createRelation(sqlContext, new CaseInsensitiveMap(options))
        } else {
          dataSource
            .asInstanceOf[org.apache.spark.sql.sources.SchemaRelationProvider]
            .createRelation(
              sqlContext, new CaseInsensitiveMap(options), Some(StructType(tableCols)))
        }
    }

    sqlContext.baseRelationToSchemaRDD(relation).registerTempTable(tableName)
    Seq.empty
  }
}
