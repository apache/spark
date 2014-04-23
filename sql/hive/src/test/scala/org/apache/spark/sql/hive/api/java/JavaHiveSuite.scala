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

package org.apache.spark.sql.hive.api.java

import org.scalatest.FunSuite

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.hive.test.TestHive

// Implicits
import scala.collection.JavaConversions._

class JavaHiveSQLSuite extends FunSuite {
  ignore("SELECT * FROM src") {
    val javaCtx = new JavaSparkContext(TestSQLContext.sparkContext)
    // There is a little trickery here to avoid instantiating two HiveContexts in the same JVM
    val javaSqlCtx = new JavaHiveContext(javaCtx) {
      override val sqlContext = TestHive
    }

    assert(
      javaSqlCtx.hql("SELECT * FROM src").collect().map(_.getInt(0)) ===
        TestHive.sql("SELECT * FROM src").collect().map(_.getInt(0)).toSeq)
  }
}
