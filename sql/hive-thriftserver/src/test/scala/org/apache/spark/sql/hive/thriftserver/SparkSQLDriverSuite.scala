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

import java.util.Collections

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.catalog.procedures.{BoundProcedure, ProcedureParameter, UnboundProcedure}
import org.apache.spark.sql.connector.read.{LocalScan, Scan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.util.Utils.REDACTION_REPLACEMENT_TEXT

class SparkSQLDriverSuite extends SharedSparkSession {

  test("job description should be redacted by spark.sql.redaction.string.regex") {
    withSQLConf(SQLConf.SQL_STRING_REDACTION_PATTERN.key -> "password=([^\\s]+)") {
      var jobDescription: String = null
      spark.sparkContext.addSparkListener(new SparkListener {
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          jobDescription =
            jobStart.properties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION)
        }
      })

      val driver = new SparkSQLDriver(spark)
      try {
        driver.run("SELECT 'password=secret123'")
      } finally {
        driver.close()
      }

      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(jobDescription != null)
      assert(!jobDescription.contains("secret123"))
      assert(jobDescription.contains(REDACTION_REPLACEMENT_TEXT))
    }
  }

  test("CALL result schema reports the procedure's output columns") {
    withSQLConf("spark.sql.catalog.cat" -> classOf[InMemoryCatalog].getName) {
      val catalog =
        spark.sessionState.catalogManager.catalog("cat").asInstanceOf[InMemoryCatalog]
      catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
      val driver = new SparkSQLDriver(spark)
      try {
        // The procedure runs at execution time, so the reported schema must come from the executed
        // plan (the procedure's columns), not the empty output of the `Call` logical node.
        driver.run("CALL cat.ns.sum(5, 5)")
        val fields = driver.getSchema.getFieldSchemas
        assert(fields.size() == 1)
        assert(fields.get(0).getName == "out")
        assert(fields.get(0).getType == "int")
      } finally {
        driver.close()
        spark.sessionState.catalogManager.reset()
      }
    }
  }

  object UnboundSum extends UnboundProcedure {
    override def name: String = "sum"
    override def description: String = "sum integers"
    override def bind(inputType: StructType): BoundProcedure = Sum
  }

  object Sum extends BoundProcedure {
    override def name: String = "sum"
    override def description: String = "sum integers"
    override def isDeterministic: Boolean = true
    override def parameters: Array[ProcedureParameter] = Array(
      ProcedureParameter.in("in1", DataTypes.IntegerType).build(),
      ProcedureParameter.in("in2", DataTypes.IntegerType).build())
    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      val result = Result(
        new StructType().add("out", DataTypes.IntegerType),
        Array(InternalRow(input.getInt(0) + input.getInt(1))))
      Collections.singleton[Scan](result).iterator()
    }
  }

  case class Result(readSchema: StructType, rows: Array[InternalRow]) extends LocalScan
}
