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

import java.io.{FileNotFoundException, IOException}

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object HiveRules {

  case class CTASWriteChecker(session: SparkSession)
    extends (LogicalPlan => Unit) with Logging {

    def failAnalysis(msg: String): Unit = {
      throw new AnalysisException(msg)
    }

    override def apply(plan: LogicalPlan) : Unit = {
      plan.foreach {
        case CreateHiveTableAsSelectCommand(tableDesc, _, _, _) =>
          val tableExists = session.sessionState.catalog.tableExists(tableDesc.identifier)
          val location = tableDesc.storage.locationUri
          if (!tableExists && location.isDefined && location.get.toString.length != 0) {
            try {
              val hadoopConfiguration = session.sessionState.newHadoopConf()
              val path = new Path(location.get)
              val fs = path.getFileSystem(hadoopConfiguration)
              val locStatus: FileStatus = if (fs != null) {
                fs.getFileStatus(path)
              } else {
                null
              }
              if (locStatus != null && locStatus.isDirectory) {
                val lStats = fs.listStatus(path)
                if (lStats != null && lStats.nonEmpty) {
                  val stagingDir = hadoopConfiguration.get("hive.exec.stagingdir", ".hive-staging")
                  // Don't throw an exception if the target location only contains the staging-dirs
                  lStats.foreach { lStat =>
                    if (!lStat.getPath.getName.startsWith(stagingDir)) {
                      failAnalysis("CREATE-TABLE-AS-SELECT cannot create table with " +
                        s"location to a non-empty directory $path.")
                    }
                  }
                }
              }
            } catch {
              case nfe: FileNotFoundException =>
              // we will create the folder if it does not exist.
              case ioE: IOException =>
                logDebug("Exception when validate folder ", ioE);
            }
          }
        case _ => // OK
      }
    }
  }
}
