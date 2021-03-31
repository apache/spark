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

package org.apache.spark.sql.hive.thriftserver

import java.sql.SQLException

import org.apache.hive.service.cli.HiveSQLException

import org.apache.spark.{SparkException, SparkUpgradeException}

private[hive] object SQLExceptionUtils {

  def toHiveSQLException(reason: String, cause: Throwable): HiveSQLException = {
    cause match {
      case e: HiveSQLException =>
        e
      case e: SparkException =>
        new HiveSQLException(reason, "undefined", 0, e)
      case e: SparkUpgradeException =>
        new HiveSQLException(reason, "undefined", 1, e)
      case e: SQLException =>
        new HiveSQLException(reason, e.getSQLState, e.getErrorCode, e)
      case e =>
        new HiveSQLException(reason, e)
    }
  }
}
