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

import dev.ludovic.netlib.arpack.{ARPACK => NetlibARPACK, JavaARPACK => NetlibJavaARPACK, NativeARPACK => NetlibNativeARPACK}

/**
 * ARPACK routines for MLlib's vectors and matrices.
 */
private[spark] object ARPACK extends Serializable {

  @transient private var _javaARPACK: NetlibARPACK = _
  @transient private var _nativeARPACK: NetlibARPACK = _

  private[spark] def javaARPACK: NetlibARPACK = {
    if (_javaARPACK == null) {
      _javaARPACK = NetlibJavaARPACK.getInstance
    }
    _javaARPACK
  }

  private[spark] def nativeARPACK: NetlibARPACK = {
    if (_nativeARPACK == null) {
      _nativeARPACK =
        try { NetlibNativeARPACK.getInstance } catch { case _: Throwable => javaARPACK }
    }
    _nativeARPACK
  }
}
