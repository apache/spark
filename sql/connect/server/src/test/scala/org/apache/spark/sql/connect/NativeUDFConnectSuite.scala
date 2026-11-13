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

import java.util.UUID

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.ffi.NativeDataSourceTestUtils.checkingOrderOfCalls
import org.apache.spark.sql.execution.datasources.v2.ffi.NativeUDFTestUtils._
import org.apache.spark.sql.functions.{array, call_function, col, transform}

/**
 * Tests native functions with Spark Connect: a client adds a package to its session with
 * `spark.addArtifact`, registers its functions with SQL and calls them by name, with no change to
 * the protocol. The tests are skipped without a C++ compiler, or before Java 22.
 */
class NativeUDFConnectSuite extends SparkConnectServerTest {

  private lazy val library = compile("test_native_udf")

  private def nativeTest(name: String)(f: => Unit): Unit = test(name) {
    assume(toolchain.isDefined, "A C++ compiler and Java 22 or later are required.")
    checkingOrderOfCalls(f)
  }

  private def withNewSession(f: SparkSession => Unit): Unit = {
    withSession(sessionId = UUID.randomUUID().toString)(f)
  }

  nativeTest("register native functions with SQL and call them by name") {
    val pkg = createPackage(library)
    withNewSession { session =>
      session.addArtifact(pkg.getPath)
      session.sql(
        s"CREATE TEMPORARY FUNCTION add_one AS 'add_one' USING PACKAGE '${pkg.getName}'")
      assert(
        session
          .sql("SELECT add_one(id) FROM range(3)")
          .collect()
          .map(_.getLong(0))
          .sorted
          .toSeq == Seq(1L, 2L, 3L))
      assert(
        session
          .range(3)
          .select(call_function("add_one", col("id")))
          .collect()
          .map(_.getLong(0))
          .sorted
          .toSeq == Seq(1L, 2L, 3L))
    }

    withNewSession { other =>
      val e = intercept[AnalysisException](other.sql("SELECT add_one(1)").collect())
      assert(e.getCondition == "UNRESOLVED_ROUTINE", e)
    }
  }

  nativeTest("reject a call in a lambda function when the client requests the schema") {
    val pkg = createPackage(library)
    withNewSession { session =>
      session.addArtifact(pkg.getPath)
      session.sql(
        s"CREATE TEMPORARY FUNCTION add_one AS 'add_one' USING PACKAGE '${pkg.getName}'")
      val df =
        session.range(2).select(transform(array(col("id")), x => call_function("add_one", x)))
      checkError(
        exception = intercept[AnalysisException](df.schema),
        condition = "UNSUPPORTED_FEATURE.NATIVE_UDF_IN_LAMBDA",
        parameters = Map("functionName" -> "`add_one`"))
    }
  }
}
