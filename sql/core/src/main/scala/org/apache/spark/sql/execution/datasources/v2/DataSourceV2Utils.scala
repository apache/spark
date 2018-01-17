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

package org.apache.spark.sql.execution.datasources.v2

import java.util.regex.Pattern

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.{DataSourceV2, SessionConfigSupport}

private[sql] object DataSourceV2Utils extends Logging {

  /**
   * Helper method that extracts and transforms session configs into k/v pairs, the k/v pairs will
   * be used to create data source options.
   * Only extract when `ds` implements [[SessionConfigSupport]], in this case we may fetch the
   * specified key-prefix from `ds`, and extract session configs with config keys that start with
   * `spark.datasource.$keyPrefix`. A session config `spark.datasource.$keyPrefix.xxx -> yyy` will
   * be transformed into `xxx -> yyy`.
   *
   * @param ds a [[DataSourceV2]] object
   * @param conf the session conf
   * @return an immutable map that contains all the extracted and transformed k/v pairs.
   */
  def extractSessionConfigs(ds: DataSourceV2, conf: SQLConf): Map[String, String] = ds match {
    case cs: SessionConfigSupport =>
      val keyPrefix = cs.keyPrefix()
      require(keyPrefix != null, "The data source config key prefix can't be null.")

      val pattern = Pattern.compile(s"^spark\\.datasource\\.$keyPrefix\\.(.+)")

      conf.getAllConfs.flatMap { case (key, value) =>
        val m = pattern.matcher(key)
        if (m.matches() && m.groupCount() > 0) {
          Seq((m.group(1), value))
        } else {
          Seq.empty
        }
      }

    case _ => Map.empty
  }

  /**
   * Helper method to parse the argument passed to load or save. If the path doesn't contain '/'
   * and cannot be a fully-qualified location, parse it as a table identifier. Otherwise, return
   * the path.
   *
   * @param sparkSession a [[SparkSession]]
   * @param pathOrTable some string passed to load or save, or None
   * @return
   */
  def parseTableLocation(
      sparkSession: SparkSession,
      pathOrTable: Option[String]): (Option[String], Option[TableIdentifier]) = {
    pathOrTable match {
      case Some(path) if !path.contains("/") =>
        // without "/", this cannot be a full path. parse it as a table name
        val ident = sparkSession.sessionState.sqlParser.parseTableIdentifier(path)
        // ensure the database is set correctly
        val db = ident.database.getOrElse(sparkSession.catalog.currentDatabase)
        (None, Some(ident.copy(database = Some(db))))

      case Some(path) =>
        (Some(path), None)

      case _ =>
        (None, None)
    }
  }
}
