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
package org.apache.spark.sql

import java.util.Arrays

import org.apache.spark.sql.connect.client.SparkResult
import org.apache.spark.sql.connect.client.util.RemoteSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class UDFClassLoadingE2ESuite extends RemoteSparkSession {

  test("load udf with default stub class loader") {
    val rows = spark.range(10).filter(n => n % 2 == 0).collectAsList()
    assert(rows == Arrays.asList[Long](0, 2, 4, 6, 8))
  }

  test("update class loader after stubbing: new session") {
    // Session1 uses Stub SparkResult class
    val session1 = spark.newSession()
    addClientTestArtifactInServerClasspath(session1)
    val ds = session1.range(10).filter(n => n % 2 == 0)

    val rows = ds.collectAsList()
    assert(rows == Arrays.asList[Long](0, 2, 4, 6, 8))

    // Session2 uses the real SparkResult class
    val session2 = spark.newSession()
    addClientTestArtifactInServerClasspath(session2)
    addClientTestArtifactInServerClasspath(session2, testJar = false)
    val rows2 = session2
      .range(10)
      .filter(n => {
        // Try to use spark result
        new SparkResult[Int](null, null, null)
        n > 5
      })
      .collectAsList()
    assert(rows2 == Arrays.asList[Long](6, 7, 8, 9))
  }

  test("update class loader after stubbing: same session") {
    val session = spark.newSession()
    addClientTestArtifactInServerClasspath(session)
    val ds = session.range(10).filter(n => n % 2 == 0)

    // load SparkResult as a stubbed class
    val rows = ds.collectAsList()
    assert(rows == Arrays.asList[Long](0, 2, 4, 6, 8))

    // Upload SparkResult and then SparkResult can be used in the udf
    addClientTestArtifactInServerClasspath(session, testJar = false)
    val rows2 = session.range(10).filter(n => {
      // Try to use spark result
      new SparkResult[Int](null, null, null)
      n > 5
    }).collectAsList()
    assert(rows2 == Arrays.asList[Long](6, 7, 8, 9))
  }

  // This dummy method generates a lambda in the test class with SparkResult in its signature.
  // This will cause class loading issue on the server side as the client jar is
  // not in the server classpath.
  def dummyMethod(): Unit = {
    val df = spark.sql("select val from (values ('Hello'), ('World')) as t(val)")
    df.withResult { result =>
      val schema = result.schema
      assert(schema == StructType(StructField("val", StringType, nullable = false) :: Nil))
    }
  }
}
