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
import org.scalatest.ConfigMap

class JoinsSuiteBase extends QueriesSuiteBase with CreateTableAndLoadData {
  self: HBaseIntegrationTestBase =>

  override protected def beforeAll(configMap: ConfigMap): Unit = {
    super.beforeAll(configMap)
    for (tx <- 1 to 0) {
      val JoinTable = s"JoinTable$tx"
      val HBaseJoinTable = s"Hb$JoinTable"
      val JoinTableStg = s"JoinTableStg$tx"
      val HBaseJoinTableStg = s"Hb$JoinTableStg"
      val JoinTableCsv: String = s"$CsvPath/JoinTable$tx.txt"
      createTables(hbc, JoinTableStg, JoinTable, HBaseJoinTableStg, HBaseJoinTable)
      loadData(hbc, JoinTableStg, JoinTable, JoinTableCsv)
    }
  }

  private val logger = Logger.getLogger(getClass.getName)

}

