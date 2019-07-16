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

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.scalatest.Assertions

import org.apache.spark.ml.param.{ParamMap, ParamPair, Params}
import org.apache.spark.ml.util.{Identifiable, MLReader, MLWritable}

object ValidatorParamsSuiteHelpers extends Assertions {
  /**
   * Assert sequences of estimatorParamMaps are identical.
   * If the values for a parameter are not directly comparable with ===
   * and are instead Params types themselves then their corresponding paramMaps
   * are compared against each other.
   */
  def compareParamMaps(pMaps: Array[ParamMap], pMaps2: Array[ParamMap]): Unit = {
    assert(pMaps.length === pMaps2.length)
    pMaps.zip(pMaps2).foreach { case (pMap, pMap2) =>
      assert(pMap.size === pMap2.size)
      pMap.toSeq.foreach { case ParamPair(p, v) =>
        assert(pMap2.contains(p))
        val otherParam = pMap2(p)
        v match {
          case estimator: Params =>
            otherParam match {
              case estimator2: Params =>
                val estimatorParamMap = Array(estimator.extractParamMap())
                val estimatorParamMap2 = Array(estimator2.extractParamMap())
                compareParamMaps(estimatorParamMap, estimatorParamMap2)
              case other =>
                fail(s"Expected parameter of type Params but found ${otherParam.getClass.getName}")
            }
          case _ =>
            assert(otherParam === v)
        }
      }
    }
  }

  /**
   * When nested estimators (ex. OneVsRest) are saved within meta-algorithms such as
   * CrossValidator and TrainValidationSplit, relative paths should be used to store
   * the path of the estimator so that if the parent directory changes, loading the
   * model still works.
   */
  def testFileMove[T <: Params with MLWritable](instance: T, tempDir: File): Unit = {
    val uid = instance.uid
    val subdirName = Identifiable.randomUID("test")

    val subdir = new File(tempDir, subdirName)
    val subDirWithUid = new File(subdir, uid)

    instance.save(subDirWithUid.getPath)

    val newSubdirName = Identifiable.randomUID("test_moved")
    val newSubdir = new File(tempDir, newSubdirName)
    val newSubdirWithUid = new File(newSubdir, uid)

    Files.createDirectory(newSubdir.toPath)
    Files.createDirectory(newSubdirWithUid.toPath)
    Files.move(subDirWithUid.toPath, newSubdirWithUid.toPath, StandardCopyOption.ATOMIC_MOVE)

    val loader = instance.getClass.getMethod("read").invoke(null).asInstanceOf[MLReader[T]]
    val newInstance = loader.load(newSubdirWithUid.getPath)
    assert(uid == newInstance.uid)
  }
}
