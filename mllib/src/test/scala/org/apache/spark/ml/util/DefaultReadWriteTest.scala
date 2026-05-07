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

import java.io.{File, IOException}

import org.scalatest.Suite

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Dataset

trait DefaultReadWriteTest extends TempDirectory { self: Suite =>

  /**
   * Checks "overwrite" option and params.
   * This saves to and loads from [[tempDir]], but creates a subdirectory with a random name
   * in order to avoid conflicts from multiple calls to this method.
   *
   * @param instance ML instance to test saving/loading
   * @param testParams  If true, then test values of Params.  Otherwise, just test overwrite option.
   * @tparam T ML instance type
   * @return  Instance loaded from file
   */
  def testDefaultReadWrite[T <: Params with MLWritable](
      instance: T,
      testParams: Boolean = true,
      testSaveToLocal: Boolean = false): T = {
    val uid = instance.uid
    val subdirName = Identifiable.randomUID("test")

    val subdir = new File(tempDir, subdirName)
    val path = new File(subdir, uid).getPath

    if (testSaveToLocal) {
      instance.write.saveToLocal(path)
      assert(
        new File(path, "metadata").isFile(),
        "saveToLocal should generate metadata as a file."
      )
      intercept[IOException] {
        instance.write.saveToLocal(path)
      }
      instance.write.overwrite().saveToLocal(path)
    } else {
      instance.save(path)
      intercept[IOException] {
        instance.save(path)
      }
      instance.write.overwrite().save(path)
    }

    val loader = instance.getClass.getMethod("read").invoke(null).asInstanceOf[MLReader[T]]
    val newInstance = if (testSaveToLocal) {
      loader.loadFromLocal(path)
    } else {
      loader.load(path)
    }
    assert(newInstance.uid === instance.uid)
    if (testParams) {
      instance.params.foreach { p =>
        if (instance.isDefined(p)) {
          (instance.getOrDefault(p), newInstance.getOrDefault(p)) match {
            case (Array(values), Array(newValues)) =>
              assert(values === newValues, s"Values do not match on param ${p.name}.")
            case (value: Double, newValue: Double) =>
              assert(value.isNaN && newValue.isNaN || value == newValue,
                s"Values do not match on param ${p.name}.")
            case (value, newValue) =>
              assert(value === newValue, s"Values do not match on param ${p.name}.")
          }
        } else {
          assert(!newInstance.isDefined(p), s"Param ${p.name} shouldn't be defined.")
        }
      }
    }
    val another = if (testSaveToLocal) {
      val read = instance.getClass.getMethod("read")
      val reader = read.invoke(instance).asInstanceOf[MLReader[T]]
      reader.loadFromLocal(path)
    } else {
      val load = instance.getClass.getMethod("load", classOf[String])
      load.invoke(instance, path).asInstanceOf[T]
    }
    assert(another.uid === instance.uid)
    another
  }

  /**
   * Default test for Estimator, Model pairs:
   *  - Explicitly set Params, and train model
   *  - Test save/load using `testDefaultReadWrite` on Estimator and Model
   *  - Check Params on Estimator and Model
   *  - Compare model data
   *
   * This requires that `Model`'s `Param`s should be a subset of `Estimator`'s `Param`s.
   *
   * @param estimator  Estimator to test
   * @param dataset  Dataset to pass to `Estimator.fit()`
   * @param testEstimatorParams  Set of `Param` values to set in estimator
   * @param testModelParams Set of `Param` values to set in model
   * @param checkModelData  Method which takes the original and loaded `Model` and compares their
   *                        data.  This method does not need to check `Param` values.
   * @tparam E  Type of `Estimator`
   * @tparam M  Type of `Model` produced by estimator
   */
  def testEstimatorAndModelReadWrite[
    E <: Estimator[M] with MLWritable, M <: Model[M] with MLWritable](
      estimator: E,
      dataset: Dataset[_],
      testEstimatorParams: Map[String, Any],
      testModelParams: Map[String, Any],
      checkModelData: (M, M) => Unit,
      skipTestSaveLocal: Boolean = false): Unit = {
    // Set some Params to make sure set Params are serialized.
    testEstimatorParams.foreach { case (p, v) =>
      estimator.set(estimator.getParam(p), v)
    }
    val model = estimator.fit(dataset)

    // Test Estimator save/load
    val estimator2 = testDefaultReadWrite(estimator)
    testEstimatorParams.foreach { case (p, v) =>
      val param = estimator.getParam(p)
      assert(estimator.get(param).get === estimator2.get(param).get)
    }

    // Test Model save/load
    val testTargets = if (skipTestSaveLocal) {
      Seq(false)
    } else {
      Seq(false, true)
    }
    for (testSaveToLocal <- testTargets) {
      val model2 = testDefaultReadWrite(model, testSaveToLocal = testSaveToLocal)
      testModelParams.foreach { case (p, v) =>
        val param = model.getParam(p)
        assert(model.get(param).get === model2.get(param).get)
      }

      checkModelData(model, model2)
    }
  }
}

class MyParams(override val uid: String) extends Params with MLWritable {

  final val intParamWithDefault: IntParam = new IntParam(this, "intParamWithDefault", "doc")
  final val shouldNotSetIfSetintParamWithDefault: IntParam =
    new IntParam(this, "shouldNotSetIfSetintParamWithDefault", "doc")
  final val intParam: IntParam = new IntParam(this, "intParam", "doc")
  final val floatParam: FloatParam = new FloatParam(this, "floatParam", "doc")
  final val doubleParam: DoubleParam = new DoubleParam(this, "doubleParam", "doc")
  final val longParam: LongParam = new LongParam(this, "longParam", "doc")
  final val stringParam: Param[String] = new Param[String](this, "stringParam", "doc")
  final val intArrayParam: IntArrayParam = new IntArrayParam(this, "intArrayParam", "doc")
  final val doubleArrayParam: DoubleArrayParam =
    new DoubleArrayParam(this, "doubleArrayParam", "doc")
  final val stringArrayParam: StringArrayParam =
    new StringArrayParam(this, "stringArrayParam", "doc")

  setDefault(intParamWithDefault -> 0)
  set(intParam -> 1)
  set(floatParam -> 2.0f)
  set(doubleParam -> 3.0)
  set(longParam -> 4L)
  set(stringParam -> "5")
  set(intArrayParam -> Array(6, 7))
  set(doubleArrayParam -> Array(8.0, 9.0))
  set(stringArrayParam -> Array("10", "11"))

  def checkExclusiveParams(): Unit = {
    if (isSet(shouldNotSetIfSetintParamWithDefault) && isSet(intParamWithDefault)) {
      throw new SparkException("intParamWithDefault and shouldNotSetIfSetintParamWithDefault " +
        "shouldn't be set at the same time")
    }
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)

  override def write: MLWriter = new DefaultParamsWriter(this)
}

object MyParams extends MLReadable[MyParams] {

  override def read: MLReader[MyParams] = new DefaultParamsReader[MyParams]

  override def load(path: String): MyParams = super.load(path)
}

class DefaultReadWriteSuite extends SparkFunSuite with MLlibTestSparkContext
  with DefaultReadWriteTest {

  test("default read/write") {
    val myParams = new MyParams("my_params")
    testDefaultReadWrite(myParams)
  }

  test("default param shouldn't become user-supplied param after persistence") {
    val myParams = new MyParams("my_params")
    myParams.set(myParams.shouldNotSetIfSetintParamWithDefault, 1)
    myParams.checkExclusiveParams()
    val loadedMyParams = testDefaultReadWrite(myParams)
    loadedMyParams.checkExclusiveParams()
    assert(loadedMyParams.getDefault(loadedMyParams.intParamWithDefault) ==
      myParams.getDefault(myParams.intParamWithDefault))

    loadedMyParams.set(myParams.intParamWithDefault, 1)
    intercept[SparkException] {
      loadedMyParams.checkExclusiveParams()
    }
  }

  test("User-supplied value for default param should be kept after persistence") {
    val myParams = new MyParams("my_params")
    myParams.set(myParams.intParamWithDefault, 100)
    val loadedMyParams = testDefaultReadWrite(myParams)
    assert(loadedMyParams.get(myParams.intParamWithDefault).get == 100)
  }

  test("Read metadata without default field prior to 2.4") {
    // default params are saved in `paramMap` field in metadata file prior to Spark 2.4.
    val metadata = """{"class":"org.apache.spark.ml.util.MyParams",
      |"timestamp":1518852502761,"sparkVersion":"2.3.0",
      |"uid":"my_params",
      |"paramMap":{"intParamWithDefault":0}}""".stripMargin
    val parsedMetadata = DefaultParamsReader.parseMetadata(metadata)
    val myParams = new MyParams("my_params")
    assert(!myParams.isSet(myParams.intParamWithDefault))
    parsedMetadata.getAndSetParams(myParams)

    // The behavior prior to Spark 2.4, default params are set in loaded ML instance.
    assert(myParams.isSet(myParams.intParamWithDefault))
  }

  test("Should raise error when read metadata without default field after Spark 2.4") {
    val myParams = new MyParams("my_params")

    val metadata1 = """{"class":"org.apache.spark.ml.util.MyParams",
      |"timestamp":1518852502761,"sparkVersion":"2.4.0",
      |"uid":"my_params",
      |"paramMap":{"intParamWithDefault":0}}""".stripMargin
    val parsedMetadata1 = DefaultParamsReader.parseMetadata(metadata1)
    val err1 = intercept[IllegalArgumentException] {
      parsedMetadata1.getAndSetParams(myParams)
    }
    assert(err1.getMessage().contains("Cannot recognize JSON metadata"))

    val metadata2 = """{"class":"org.apache.spark.ml.util.MyParams",
      |"timestamp":1518852502761,"sparkVersion":"3.0.0",
      |"uid":"my_params",
      |"paramMap":{"intParamWithDefault":0}}""".stripMargin
    val parsedMetadata2 = DefaultParamsReader.parseMetadata(metadata2)
    val err2 = intercept[IllegalArgumentException] {
      parsedMetadata2.getAndSetParams(myParams)
    }
    assert(err2.getMessage().contains("Cannot recognize JSON metadata"))
  }
}
