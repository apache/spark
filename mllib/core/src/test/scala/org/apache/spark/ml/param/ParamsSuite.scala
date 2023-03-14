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

package org.apache.spark.ml.param

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.ml.util.MyParams
import org.apache.spark.sql.Dataset

class ParamsSuite extends SparkFunSuite {

  test("Filtering ParamMap") {
    val params1 = new MyParams("my_params1")
    val params2 = new MyParams("my_params2")
    val paramMap = ParamMap(
      params1.intParam -> 1,
      params2.intParam -> 1,
      params1.doubleParam -> 0.2,
      params2.doubleParam -> 0.2)
    val filteredParamMap = paramMap.filter(params1)

    assert(filteredParamMap.size === 2)
    filteredParamMap.toSeq.foreach {
      case ParamPair(p, _) =>
        assert(p.parent === params1.uid)
    }

    // At the previous implementation of ParamMap#filter,
    // mutable.Map#filterKeys was used internally but
    // the return type of the method is not serializable (see SI-6654).
    // Now mutable.Map#filter is used instead of filterKeys and the return type is serializable.
    // So let's ensure serializability.
    val objOut = new ObjectOutputStream(new ByteArrayOutputStream())
    objOut.writeObject(filteredParamMap)
  }
}

object ParamsSuite extends SparkFunSuite {

  /**
   * Checks common requirements for `Params.params`:
   *   - params are ordered by names
   *   - param parent has the same UID as the object's UID
   *   - param name is the same as the param method name
   *   - obj.copy should return the same type as the obj
   */
  def checkParams(obj: Params): Unit = {
    val clazz = obj.getClass

    val params = obj.params
    val paramNames = params.map(_.name)
    require(paramNames === paramNames.sorted, "params must be ordered by names")
    params.foreach { p =>
      assert(p.parent === obj.uid)
      assert(obj.getParam(p.name) === p)
      // TODO: Check that setters return self, which needs special handling for generic types.
    }

    val copyMethod = clazz.getMethod("copy", classOf[ParamMap])
    val copyReturnType = copyMethod.getReturnType
    require(copyReturnType === obj.getClass,
      s"${clazz.getName}.copy should return ${clazz.getName} instead of ${copyReturnType.getName}.")
  }

  /**
   * Checks that the class throws an exception in case multiple exclusive params are set.
   * The params to be checked are passed as arguments with their value.
   */
  def testExclusiveParams(
      model: Params,
      dataset: Dataset[_],
      paramsAndValues: (String, Any)*): Unit = {
    val m = model.copy(ParamMap.empty)
    paramsAndValues.foreach { case (paramName, paramValue) =>
      m.set(m.getParam(paramName), paramValue)
    }
    intercept[IllegalArgumentException] {
      m match {
        case t: Transformer => t.transform(dataset)
        case e: Estimator[_] => e.fit(dataset)
      }
    }
  }
}
