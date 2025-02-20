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

package org.apache.spark.sql.connect

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.Row
import org.apache.spark.sql.connect.test.{QueryTest, RemoteSparkSession}

class ProcedureSuite extends QueryTest with RemoteSparkSession with BeforeAndAfter {

  test("each call should invoke procedure once") {
    checkAnswer(sql("CALL testcat.dummy.increment()"), Row(1))
    checkAnswer(sql("CALL testcat.dummy.increment()"), Row(2))
  }
}
