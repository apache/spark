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
import org.apache.spark.ml.param._
import org.apache.spark.mllib.util.MLlibTestSparkContext

trait DefaultReadWriteTest extends TempDirectory { self: Suite =>

  /**
   * Checks "overwrite" option and params.
   * @param instance ML instance to test saving/loading
   * @tparam T ML instance type
   * @return  Instance loaded from file
   */
  def testDefaultReadWrite[T <: Params with Writable](instance: T): T = {
    val uid = instance.uid
    val path = new File(tempDir, uid).getPath

    instance.save(path)
    intercept[IOException] {
      instance.save(path)
    }
    instance.write.overwrite().save(path)
    val loader = instance.getClass.getMethod("read").invoke(null).asInstanceOf[Reader[T]]
    val newInstance = loader.load(path)

    assert(newInstance.uid === instance.uid)
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

    val load = instance.getClass.getMethod("load", classOf[String])
    val another = load.invoke(instance, path).asInstanceOf[T]
    assert(another.uid === instance.uid)
    another
  }
}

class MyParams(override val uid: String) extends Params with Writable {

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

  override def write: Writer = new DefaultParamsWriter(this)
}

object MyParams extends Readable[MyParams] {

  override def read: Reader[MyParams] = new DefaultParamsReader[MyParams]

  override def load(path: String): MyParams = read.load(path)
}

class DefaultReadWriteSuite extends SparkFunSuite with MLlibTestSparkContext
  with DefaultReadWriteTest {

  test("default read/write") {
    val myParams = new MyParams("my_params")
    testDefaultReadWrite(myParams)
  }
}
