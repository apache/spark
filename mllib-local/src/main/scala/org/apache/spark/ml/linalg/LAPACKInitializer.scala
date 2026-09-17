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

package org.apache.spark.ml.linalg

import breeze.linalg.{eigSym, DenseMatrix => BDM}

private[spark] object LAPACKInitializer {

  // F2J's machine-parameter discovery uses unsynchronized mutable static state. A concurrent
  // first invocation can observe partially initialized values and make some LAPACK routines loop
  // forever. JVM class initialization serializes this warm-up before real calls run concurrently.
  // It runs once per Spark classloader in each executor JVM: the first thread performs the warm-up
  // while concurrent threads wait for class initialization to finish. Later initialize() calls are
  // cheap reads of the completed Unit field. In local mode, the driver and executor share the JVM,
  // so the warm-up also runs only once there.
  //
  // The warm-up uses Breeze's selected LAPACK backend. It is required only for F2J; a native
  // provider such as OpenBLAS or MKL only incurs this one-time 2-by-2 decomposition. Later calls
  // remain concurrent. A 2-by-2 matrix is used because DSYEV returns early for a 1-by-1 matrix
  // without initializing floating-point limits such as epsilon, the safe minimum, and the maximum
  // finite value.
  private val initialized: Unit = {
    eigSym(BDM.eye[Double](2))
  }

  def initialize(): Unit = initialized
}
