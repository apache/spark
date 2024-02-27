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

import org.apache.hive.service.cli.operation.MetadataOperationUtils

import org.apache.spark.SparkFunSuite

class MetadataOperationUtilsSuite extends SparkFunSuite {

  test("convertSchemaPattern") {
    Seq(("", "*"), ("%", "*"), (null, "*"),
      (".*", ".*"), ("_*", ".*"), ("_%", ".*"), (".%", ".*"),
      ("db%", "db*"), ("db*", "db*"),
      ("db_", "db."), ("db.", "db."),
      ("*", "*")) foreach { v =>
        val schemaPattern = MetadataOperationUtils.convertSchemaPattern(v._1)
        assert(schemaPattern == v._2)
    }
  }

  test("newConvertSchemaPattern") {
    Seq(("", "%", "%"), ("%", "%", "%"), (null, "%", "%"),
      (".*", "_%", "%"), ("_*", "_%", "_*"), ("_%", "_%", "_%"), (".%", "_%", "_%"),
      ("db%", "db%", "db%"), ("db*", "db%", "db*"),
      ("db_", "db_", "db_"), ("db.", "db_", "db_"),
      ("*", "%", "*")) foreach { v =>
      val schemaPattern = MetadataOperationUtils.newConvertSchemaPattern(v._1, true)
      assert(schemaPattern == v._2)

      val schemaPatternDatanucleusFormat =
        MetadataOperationUtils.newConvertSchemaPattern(v._1, false)
      assert(schemaPatternDatanucleusFormat == v._3)
    }
  }
}
