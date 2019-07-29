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

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkContext
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object HiveRules {

  object CTASWriteChecker extends (LogicalPlan => Unit) {

    def failAnalysis(msg: String): Unit = {
      throw new AnalysisException(msg)
    }

    override def apply(plan: LogicalPlan): Unit = {
      plan.foreach {
        case CreateHiveTableAsSelectCommand(tableDesc, _, _, _) => {
          val location = tableDesc.storage.locationUri
          if (location.isDefined) {
            val path = new Path(location.get)
            val fs = FileSystem.get(location.get, SparkContext.getActive.get.hadoopConfiguration)
            if (fs.exists(path)) {
              if (fs.isDirectory(path)) {
                failAnalysis("Creating table as select with a existed location of directory is not allowed, " +
                  "please check the path and try again")
              } else {
                failAnalysis("Creating table as select with a existed location of file is not allowed, " +
                  "please check the path and try again")
              }
            }
          }
        }

        case _ => // OK
      }
    }
  }

}
