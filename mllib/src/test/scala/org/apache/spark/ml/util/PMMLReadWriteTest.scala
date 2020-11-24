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

import org.dmg.pmml.PMML
import org.scalatest.Suite

import org.apache.spark.SparkContext
import org.apache.spark.ml.param._

trait PMMLReadWriteTest extends TempDirectory { self: Suite =>
  /**
   * Test PMML export. Requires exported model is small enough to be loaded locally.
   * Checks that the model can be exported and the result is valid PMML, but does not check
   * the specific contents of the model.
   */
  def testPMMLWrite[T <: Params with GeneralMLWritable](sc: SparkContext, instance: T,
    checkModelData: PMML => Unit): Unit = {
    val uid = instance.uid
    val subdirName = Identifiable.randomUID("pmml-")

    val subdir = new File(tempDir, subdirName)
    val path = new File(subdir, uid).getPath

    instance.write.format("pmml").save(path)
    intercept[IOException] {
      instance.write.format("pmml").save(path)
    }
    instance.write.format("pmml").overwrite().save(path)
    val pmmlStr = sc.textFile(path).collect.mkString("\n")
    val pmmlModel = PMMLUtils.loadFromString(pmmlStr)
    assert(pmmlModel.getHeader().getApplication().getName().startsWith("Apache Spark"))
    checkModelData(pmmlModel)
  }
}
