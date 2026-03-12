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

package org.apache.spark.sql.jdbc

import org.apache.spark.sql.NestedDataSourceSuiteBase
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class JDBCNestedDataSourceSuite extends NestedDataSourceSuiteBase {
  override val nestedDataSources: Seq[String] = Seq("jdbc")
  private val tempDir = Utils.createTempDir()
  private val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
  override val colType: String = "in the customSchema option value"

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  override def readOptions(schema: StructType): Map[String, String] = {
    Map("url" -> url, "dbtable" -> "t1", "customSchema" -> schema.toDDL)
  }

  override def save(selectExpr: Seq[String], format: String, path: String): Unit = {
    // We ignore `selectExpr` because:
    //  1. H2 doesn't support nested columns
    //  2. JDBC datasource checks duplicates before comparing of user's schema with
    //     actual schema of `t1`.
    spark
      .range(1L)
      .write.mode("overwrite")
      .options(Map("url" -> url, "dbtable" -> "t1"))
      .format(format)
      .save()
  }
}
