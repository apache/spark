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

package org.apache.spark.ml.util

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.util.LinearDataGenerator
import org.apache.spark.sql.{DataFrame, SparkSession}

class FakeLinearRegressionWriter extends MLWriterFormat {
  override def write(path: String, sparkSession: SparkSession,
    optionMap: mutable.Map[String, String], stage: PipelineStage): Unit = {
    throw new Exception(s"Fake writer doesn't writestart")
  }
}

class FakeLinearRegressionWriterWithName extends MLFormatRegister {
  override def format(): String = "fakeWithName"
  override def stageName(): String = "org.apache.spark.ml.regression.LinearRegressionModel"
  override def write(path: String, sparkSession: SparkSession,
    optionMap: mutable.Map[String, String], stage: PipelineStage): Unit = {
    throw new Exception(s"Fake writer doesn't writestart")
  }
}


class DuplicateLinearRegressionWriter1 extends MLFormatRegister {
  override def format(): String = "dupe"
  override def stageName(): String = "org.apache.spark.ml.regression.LinearRegressionModel"
  override def write(path: String, sparkSession: SparkSession,
    optionMap: mutable.Map[String, String], stage: PipelineStage): Unit = {
    throw new Exception(s"Duplicate writer shouldn't have been called")
  }
}

class DuplicateLinearRegressionWriter2 extends MLFormatRegister {
  override def format(): String = "dupe"
  override def stageName(): String = "org.apache.spark.ml.regression.LinearRegressionModel"
  override def write(path: String, sparkSession: SparkSession,
    optionMap: mutable.Map[String, String], stage: PipelineStage): Unit = {
    throw new Exception(s"Duplicate writer shouldn't have been called")
  }
}

class ReadWriteSuite extends MLTest {

  import testImplicits._

  private val seed: Int = 42
  @transient var dataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataset = sc.parallelize(LinearDataGenerator.generateLinearInput(
      intercept = 0.0, weights = Array(1.0, 2.0), xMean = Array(0.0, 1.0),
      xVariance = Array(2.0, 1.0), nPoints = 10, seed, eps = 0.2)).map(_.asML).toDF()
  }

  test("unsupported/non existent export formats") {
    val lr = new LinearRegression()
    val model = lr.fit(dataset)
    // Does not exist with a long class name
    val thrownDNE = intercept[SparkException] {
      model.write.format("com.holdenkarau.boop").save("boop")
    }
    assert(thrownDNE.getMessage().
      contains("Could not load requested format"))

    // Does not exist with a short name
    val thrownDNEShort = intercept[SparkException] {
      model.write.format("boop").save("boop")
    }
    assert(thrownDNEShort.getMessage().
      contains("Could not load requested format"))

    // Check with a valid class that is not a writer format.
    val thrownInvalid = intercept[SparkException] {
      model.write.format("org.apache.spark.SparkContext").save("boop2")
    }
    assert(thrownInvalid.getMessage()
      .contains("ML source org.apache.spark.SparkContext is not a valid MLWriterFormat"))
  }

  test("invalid paths fail") {
    val lr = new LinearRegression()
    val model = lr.fit(dataset)
    val thrown = intercept[Exception] {
      model.write.format("pmml").save("")
    }
    assert(thrown.getMessage().contains("Can not create a Path from an empty string"))
  }

  test("dummy export format is called") {
    val lr = new LinearRegression()
    val model = lr.fit(dataset)
    val thrown = intercept[Exception] {
      model.write.format("org.apache.spark.ml.util.FakeLinearRegressionWriter").save("name")
    }
    assert(thrown.getMessage().contains("Fake writer doesn't write"))
    val thrownWithName = intercept[Exception] {
      model.write.format("fakeWithName").save("name")
    }
    assert(thrownWithName.getMessage().contains("Fake writer doesn't write"))
  }

  test("duplicate format raises error") {
    val lr = new LinearRegression()
    val model = lr.fit(dataset)
    val thrown = intercept[Exception] {
      model.write.format("dupe").save("dupepanda")
    }
    assert(thrown.getMessage().contains("Multiple writers found for"))
  }
}
