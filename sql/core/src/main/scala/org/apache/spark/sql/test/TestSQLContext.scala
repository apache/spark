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

package org.apache.spark.sql.test

import scala.language.implicitConversions

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SchemaRDD, SQLConf, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** A SQLContext that can be used for local testing. */
object TestSQLContext
  extends SQLContext(
    new SparkContext(
      "local[2]",
      "TestSQLContext",
      new SparkConf().set("spark.sql.testkey", "true"))) {

  /** Fewer partitions to speed up testing. */
  protected[sql] override lazy val conf: SQLConf = new SQLConf {
    override def numShufflePartitions: Int = this.getConf(SQLConf.SHUFFLE_PARTITIONS, "5").toInt
  }

  /**
   * Turn a logical plan into a SchemaRDD. This should be removed once we have an easier way to
   * construct SchemaRDD directly out of local data without relying on implicits.
   */
  protected[sql] implicit def logicalPlanToSparkQuery(plan: LogicalPlan): SchemaRDD = {
    new SchemaRDD(this, plan)
  }

}
