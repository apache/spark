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

package org.apache.spark.ml.tuning

import org.apache.spark.ml.param._
import org.apache.spark.SparkFunSuite


class RandomParamGridBuilderSuite extends SparkFunSuite {

  val nRandomnessChecks = 1000

  test("random grid builder: should make the right amount of grids of Java params") {
    val nIterations = 6
    val param1 = new IntParam("parent", "param1", "doc")
    val param2 = new DoubleParam("parent", "param2", "doc")

    val builder = new RandomParamGridBuilder(Some(4))
    val paramMaps = builder
      .addUniformDistribution(param1, 10, 100)
      .addUniformDistribution(param2, 0.001, 0.1)
      .build(nIterations)

    assert(paramMaps.length === nIterations)
  }

  test("random grid builder: should make the right amount of grids of Param[_]s") {
    val nIterations = 6
    val intParam = new Param[Int]("parent", "intParam", "doc")
    val doubleParam = new Param[Double]("parent", "doubleParam", "doc")
    val booleanParam = new Param[Boolean]("parent", "booleanParam", "doc")
    val stringParam = new Param[String]("parent", "stringParam", "doc")

    val builder = new RandomParamGridBuilder(Some(4))
    val paramMaps = builder
      .addUniformDistribution(intParam, 10, 100)
      .addUniformDistribution(doubleParam, 0.001, 0.1)
      .addUniformDistribution(booleanParam)
      .addUniformChoice(stringParam, Array("one", "two", "three"))
      .build(nIterations)

    assert(paramMaps.length === nIterations)
  }
  
  test("random grid builder: each build call should generate a new set of random values") {
    val nIterations = 6
    val floatParam = new Param[Float]("parent", "floatParam", "doc")
    val doubleParam = new Param[Double]("parent", "doubleParam", "doc")

    val builder = new RandomParamGridBuilder(Some(4))
    builder
      .addUniformDistribution(doubleParam, 0.001, 0.1)
      .addUniformDistribution(floatParam, 0.001f, 0.1f)

    val grids1 = builder.build(3)
    val grids2 = builder.build(3)

    val grids1Floats = grids1.map(_.getOrElse[Float](floatParam, -1f))
    val grids2Floats = grids2.map(_.getOrElse[Float](floatParam, -1f))
    assert(!grids1Floats.sameElements(grids2Floats))

    val grids1Doubles = grids1.map(_.getOrElse[Double](doubleParam, -1))
    val grids2Doubles = grids2.map(_.getOrElse[Double](doubleParam, -1))
    assert(!grids1Doubles.sameElements(grids2Doubles))
  }

  test("addDistribution: should allow me to add functions with different outputs") {
    val builder = new RandomParamGridBuilder(Some(4))
    val param = new IntParam("parent", "param", "doc")
    def f(): Int = 10

    val paramMap = builder.addDistribution(param, f)
      .build(5)

    paramMap.map(_.getOrElse(param, -1))
      .foreach(v => assert(v == 10))
  }

  test("addUniformDistribution: should calculate random floats within limits") {
    val builder = new RandomParamGridBuilder(Some(4))
    val param = new FloatParam("parent", "param", "doc")
    val paramMap = builder.addUniformDistribution(param, 3f, 11f).build(nRandomnessChecks)

    val randomNums = paramMap.map(_.getOrElse[Float](param, -1))
    assert(randomNums.filter(_ > 11f).length === 0)
    assert(randomNums.filter(_ < 3f).length === 0)
  }

  test("addUniformDistribution: should calculate random doubles within limits") {
    val builder = new RandomParamGridBuilder(Some(4))
    val param = new DoubleParam("parent", "param", "doc")
    val paramMap = builder.addUniformDistribution(param, 3d, 11d).build(nRandomnessChecks)

    val randomNums = paramMap.map(_.getOrElse[Double](param, -1))
    assert(randomNums.filter(_ > 11).length === 0)
    assert(randomNums.filter(_ < 3).length === 0)
  }

  test("addUniformDistribution: should calculate random ints within limits") {
    val builder = new RandomParamGridBuilder(Some(4))
    val param = new IntParam("parent", "param", "doc")
    val paramMap = builder.addUniformDistribution(param, 3, 11).build(nRandomnessChecks)

    val randomNums = paramMap.map(_.getOrElse[Int](param, -1))
    assert(randomNums.filter(_ > 11).length === 0)
    assert(randomNums.filter(_ < 3).length === 0)
    assert(randomNums.filter(_ == 11).length > 0)
    assert(randomNums.filter(_ == 3).length > 0)
  }

  test("addUniformDistribution: should calculate random longs within limits") {
    val builder = new RandomParamGridBuilder(Some(4))
    val param = new LongParam("parent", "param", "doc")
    val paramMap = builder.addUniformDistribution(param, 3L, 11L).build(nRandomnessChecks)

    val randomNums = paramMap.map(_.getOrElse[Long](param, -1))
    assert(randomNums.filter(_ > 11).length === 0)
    assert(randomNums.filter(_ < 3).length === 0)
    assert(randomNums.filter(_ == 11).length > 0)
    assert(randomNums.filter(_ == 3).length > 0)
  }

  test("addUniformChoice: should choose all options") {
    val builder = new RandomParamGridBuilder(Some(4))
    val stringParam = new Param[String]("parent", "stringParam", "doc")
    val choices = Array("my", "potential", "choices")

    val paramMap = builder.addUniformChoice(stringParam, choices).build(nRandomnessChecks)
    val randomChoices = paramMap.map(_.getOrElse[String](stringParam, "oops"))

    assert(randomChoices.contains("my"))
    assert(randomChoices.contains("potential"))
    assert(randomChoices.contains("choices"))
    assert(!randomChoices.contains("oops"))
  }
}
