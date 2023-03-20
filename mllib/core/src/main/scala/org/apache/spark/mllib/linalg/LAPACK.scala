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

package org.apache.spark.mllib.linalg

import dev.ludovic.netlib.lapack.{JavaLAPACK => NetlibJavaLAPACK, LAPACK => NetlibLAPACK, NativeLAPACK => NetlibNativeLAPACK}

/**
 * LAPACK routines for MLlib's vectors and matrices.
 */
private[spark] object LAPACK extends Serializable {

  @transient private var _javaLAPACK: NetlibLAPACK = _
  @transient private var _nativeLAPACK: NetlibLAPACK = _

  private[spark] def javaLAPACK: NetlibLAPACK = {
    if (_javaLAPACK == null) {
      _javaLAPACK = NetlibJavaLAPACK.getInstance
    }
    _javaLAPACK
  }

  private[spark] def nativeLAPACK: NetlibLAPACK = {
    if (_nativeLAPACK == null) {
      _nativeLAPACK =
        try { NetlibNativeLAPACK.getInstance } catch { case _: Throwable => javaLAPACK }
    }
    _nativeLAPACK
  }
}
