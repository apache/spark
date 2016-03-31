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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.{Model, Estimator}
import org.apache.spark.ml.param._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.DataFrame

trait DefaultReadWriteTest extends TempDirectory { self: Suite =>

  /**
   * Checks "overwrite" option and params.
   * This saves to and loads from [[tempDir]], but creates a subdirectory with a random name
   * in order to avoid conflicts from multiple calls to this method.
   * @param instance ML instance to test saving/loading
   * @param testParams  If true, then test values of Params.  Otherwise, just test overwrite option.
   * @tparam T ML instance type
   * @return  Instance loaded from file
   */
  def testDefaultReadWrite[T <: Params with MLWritable](
      instance: T,
      testParams: Boolean = true): T = {
    val uid = instance.uid
    val subdirName = Identifiable.randomUID("test")

    val subdir = new File(tempDir, subdirName)
    val path = new File(subdir, uid).getPath

    instance.save(path)
    intercept[IOException] {
      instance.save(path)
    }
    instance.write.overwrite().save(path)
    val loader = instance.getClass.getMethod("read").invoke(null).asInstanceOf[MLReader[T]]
    val newInstance = loader.load(path)

    assert(newInstance.uid === instance.uid)
    if (testParams) {
      instance.params.foreach { p =>
        if (instance.isDefined(p)) {
          (instance.getOrDefault(p), newInstance.getOrDefault(p)) match {
            case (Array(values), Array(newValues)) =>
              assert(values === newValues, s"Values do not match on param ${p.name}.")
            case (value, newValue) =>
              assert(value === newValue, s"Values do not match on param ${p.name}.")
          }
        } else {
          assert(!newInstance.isDefined(p), s"Param ${p.name} shouldn't be defined.")
        }
      }
    }

    val load = instance.getClass.getMethod("load", classOf[String])
    val another = load.invoke(instance, path).asInstanceOf[T]
    assert(another.uid === instance.uid)
    another
  }

  /**
   * Default test for Estimator, Model pairs:
   *  - Explicitly set Params, and train model
   *  - Test save/load using [[testDefaultReadWrite()]] on Estimator and Model
   *  - Check Params on Estimator and Model
   *
   * This requires that the [[Estimator]] and [[Model]] share the same set of [[Param]]s.
   * @param estimator  Estimator to test
   * @param dataset  Dataset to pass to [[Estimator.fit()]]
   * @param testParams  Set of [[Param]] values to set in estimator
   * @param checkModelData  Method which takes the original and loaded [[Model]] and compares their
   *                        data.  This method does not need to check [[Param]] values.
   * @tparam E  Type of [[Estimator]]
   * @tparam M  Type of [[Model]] produced by estimator
   */
  def testEstimatorAndModelReadWrite[
    E <: Estimator[M] with MLWritable, M <: Model[M] with MLWritable](
      estimator: E,
      dataset: DataFrame,
      testParams: Map[String, Any],
      checkModelData: (M, M) => Unit): Unit = {
    // Set some Params to make sure set Params are serialized.
    testParams.foreach { case (p, v) =>
      estimator.set(estimator.getParam(p), v)
    }
    val model = estimator.fit(dataset)

    // Test Estimator save/load
    val estimator2 = testDefaultReadWrite(estimator)
    testParams.foreach { case (p, v) =>
      val param = estimator.getParam(p)
      assert(estimator.get(param).get === estimator2.get(param).get)
    }

    // Test Model save/load
    val model2 = testDefaultReadWrite(model)
    testParams.foreach { case (p, v) =>
      val param = model.getParam(p)
      assert(model.get(param).get === model2.get(param).get)
    }
  }
}

class MyParams(override val uid: String) extends Params with MLWritable {

  final val intParamWithDefault: IntParam = new IntParam(this, "intParamWithDefault", "doc")
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
}
