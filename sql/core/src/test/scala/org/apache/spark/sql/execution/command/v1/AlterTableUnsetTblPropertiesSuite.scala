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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

/**
 * This base suite contains unified tests for the `ALTER TABLE .. UNSET TBLPROPERTIES`
 * command that check V1 table catalogs. The tests that cannot run for all V1 catalogs
 * are located in more specific test suites:
 *
 *   - V1 In-Memory catalog:
 *      `org.apache.spark.sql.execution.command.v1.AlterTableUnsetTblPropertiesSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.AlterTableUnsetTblPropertiesSuite`
 */
trait AlterTableUnsetTblPropertiesSuiteBase extends command.AlterTableUnsetTblPropertiesSuiteBase {
  private[sql] lazy val sessionCatalog = spark.sessionState.catalog

  private def isUsingHiveMetastore: Boolean = {
    spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "hive"
  }

  private def normalizeTblProps(props: Map[String, String]): Map[String, String] = {
    props.filterNot(p => Seq("transient_lastDdlTime").contains(p._1))
  }

  private def getTableProperties(tableIdent: TableIdentifier): Map[String, String] = {
    sessionCatalog.getTableMetadata(tableIdent).properties
  }

  override def checkTblProps(tableIdent: TableIdentifier,
      expectedTblProps: Map[String, String]): Unit = {
    val actualTblProps = getTableProperties(tableIdent)
    if (isUsingHiveMetastore) {
      assert(normalizeTblProps(actualTblProps) == expectedTblProps)
    } else {
      assert(actualTblProps == expectedTblProps)
    }
  }

  override def getTblPropertyValue(tableIdent: TableIdentifier, key: String): String = {
    getTableProperties(tableIdent).getOrElse(key, null)
  }
}

class AlterTableUnsetTblPropertiesSuite
  extends AlterTableUnsetTblPropertiesSuiteBase with CommandSuiteBase
