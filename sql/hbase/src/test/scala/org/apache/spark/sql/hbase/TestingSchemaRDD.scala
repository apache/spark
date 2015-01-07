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
package org.apache.spark.sql.hbase

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Row, SchemaRDD}

/**
 * TestingSchemaRDD
 * Created by sboesch on 10/6/14.
 */
class TestingSchemaRDD(@transient sqlContext: HBaseSQLContext,
    @transient baseLogicalPlan: LogicalPlan)
    extends SchemaRDD(sqlContext, baseLogicalPlan) {
  @transient val logger = Logger.getLogger(getClass.getName)

  /** A private method for tests, to look at the contents of each partition */
  override private[spark] def collectPartitions(): Array[Array[Row]] = {
    sparkContext.runJob(this, (iter: Iterator[Row]) => iter.toArray, partitions.map{_.index},
      allowLocal=true)
  }

}
