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

package org.apache.spark.sql.hive.execution

import org.apache.hadoop.hive.ql.plan.CreateTableDesc

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlan, Command, LeafNode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.MetastoreRelation

/**
 * :: Experimental ::
 * Create table and insert the query result into it.
 * @param database the database name of the new relation
 * @param tableName the table name of the new relation
 * @param query the query whose result will be insert into the new relation
 * @param allowExisting allow continue working if it's already exists, otherwise
 *                      raise exception
 * @param desc the CreateTableDesc, which may contains serde, storage handler etc.

 */
@Experimental
case class CreateTableAsSelect(
    database: String,
    tableName: String,
    query: LogicalPlan,
    allowExisting: Boolean,
    desc: Option[CreateTableDesc]) extends LeafNode with Command {

  def output = Seq.empty

  private[this] def sc = sqlContext.asInstanceOf[HiveContext]

  // A lazy computing of the metastoreRelation
  private[this] lazy val metastoreRelation: MetastoreRelation = {
    // Create Hive Table
    sc.catalog.createTable(database, tableName, query.output, allowExisting, desc)

    // Get the Metastore Relation
    sc.catalog.lookupRelation(Some(database), tableName, None) match {
      case r: MetastoreRelation => r
    }
  }

  override protected[sql] lazy val sideEffectResult: Seq[Row] = {
    // TODO ideally, we should get the output data ready first and then
    // add the relation into catalog, just in case of failure occurs while data
    // processing.
    if (sc.catalog.tableExists(Some(database), tableName)) {
      if (allowExisting) {
        // table already exists, will do nothing, to keep consistent with Hive
      } else {
        throw
          new org.apache.hadoop.hive.metastore.api.AlreadyExistsException(s"$database.$tableName")
      }
    } else {
      sc.executePlan(InsertIntoTable(metastoreRelation, Map(), query, true)).toRdd
    }

    Seq.empty[Row]
  }

  override def execute(): RDD[Row] = {
    sideEffectResult
    sparkContext.emptyRDD[Row]
  }

  override def argString: String = {
    s"[Database:$database, TableName: $tableName, InsertIntoHiveTable]\n" + query.toString
  }
}
