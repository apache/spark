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

package org.apache.spark.mllib.util

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * A collection of methods used to validate data before applying ML algorithms.
 */
@DeveloperApi
@Since("0.8.0")
object DataValidators extends Logging {

  /**
   * Function to check if labels used for classification are either zero or one.
   *
   * @return True if labels are all zero or one, false otherwise.
   */
  @Since("1.0.0")
  val binaryLabelValidator: RDD[LabeledPoint] => Boolean = { data =>
    val numInvalid = data.filter(x => x.label != 1.0 && x.label != 0.0).count()
    if (numInvalid != 0) {
      logError("Classification labels should be 0 or 1. Found " + numInvalid + " invalid labels")
    }
    numInvalid == 0
  }

  /**
   * Function to check if labels used for k class multi-label classification are
   * in the range of {0, 1, ..., k - 1}.
   *
   * @return True if labels are all in the range of {0, 1, ..., k-1}, false otherwise.
   */
  @Since("1.3.0")
  def multiLabelValidator(k: Int): RDD[LabeledPoint] => Boolean = { data =>
    val numInvalid = data.filter(x =>
      x.label - x.label.toInt != 0.0 || x.label < 0 || x.label > k - 1).count()
    if (numInvalid != 0) {
      logError("Classification labels should be in {0 to " + (k - 1) + "}. " +
        "Found " + numInvalid + " invalid labels")
    }
    numInvalid == 0
  }
}
