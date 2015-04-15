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

/** A subclass of Params for testing. */
class TestParams extends Params {

  val maxIter = new IntParam(this, "maxIter", "max number of iterations")
  def setMaxIter(value: Int): this.type = { set(maxIter, value); this }
  def getMaxIter: Int = getOrDefault(maxIter)

  val inputCol = new Param[String](this, "inputCol", "input column name")
  def setInputCol(value: String): this.type = { set(inputCol, value); this }
  def getInputCol: String = getOrDefault(inputCol)

  setDefault(maxIter -> 10)

  override def validate(paramMap: ParamMap): Unit = {
    val m = extractParamMap(paramMap)
    require(m(maxIter) >= 0)
    require(m.contains(inputCol))
  }

  def clearMaxIter(): this.type = clear(maxIter)
}
