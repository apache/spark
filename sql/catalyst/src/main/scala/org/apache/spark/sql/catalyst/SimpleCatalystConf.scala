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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.internal.SQLConf


/**
 * A SQLConf that can be used for local testing. This class is only here to minimize the change
 * for ticket SPARK-19944 (moves SQLConf from sql/core to sql/catalyst). This class should
 * eventually be removed (test cases should just create SQLConf and set values appropriately).
 */
case class SimpleCatalystConf(
    override val caseSensitiveAnalysis: Boolean,
    override val orderByOrdinal: Boolean = true,
    override val groupByOrdinal: Boolean = true,
    override val optimizerMaxIterations: Int = 100,
    override val optimizerInSetConversionThreshold: Int = 10,
    override val maxCaseBranchesForCodegen: Int = 20,
    override val runSQLonFile: Boolean = true,
    override val crossJoinEnabled: Boolean = false,
    override val warehousePath: String = "/user/hive/warehouse")
  extends SQLConf
