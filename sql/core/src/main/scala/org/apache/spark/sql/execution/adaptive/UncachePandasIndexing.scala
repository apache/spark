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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

object UncachePandasIndexing extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    val sc = plan.session.sparkContext

    // TODO: add a config to specify the maximum number of cached RDDs
    sc.synchronized {
      val cachedRDDs = sc.persistentRdds.toSeq
        .filter { case (id, rdd) =>
          rdd != null &&
            rdd.name != null &&
            rdd.name.startsWith("__Pandas_AttachDistributedSequence_")
        }

      val numCached = cachedRDDs.size
      if (numCached > 1) {
        cachedRDDs.sortBy(_._1).take(numCached - 1).foreach { case (id, rdd) =>
          rdd.unpersist(blocking = false)
        }
      }
    }

    plan
  }
}
