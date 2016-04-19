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

package org.apache.spark.sql.udt

import org.apache.spark.sql.types.UDTRegistration

/**
 * This object holds the built-in User Defined Types (UDTs) which we want to pre-load
 * into UDTRegistration.
 */
private[sql] object BuiltInUDT {
  // VectorUDT and MatrixUDT in test package org.apache.spark.sql for test purpose.
  val preloadedUDT =
    ("org.apache.spark.ml.linalg.Vector", "org.apache.spark.sql.udt.VectorUDT") ::
    ("org.apache.spark.ml.linalg.DenseVector", "org.apache.spark.sql.udt.VectorUDT") ::
    ("org.apache.spark.ml.linalg.SparseVector", "org.apache.spark.sql.udt.VectorUDT") ::
    ("org.apache.spark.ml.linalg.Matrix", "org.apache.spark.sql.udt.MatrixUDT") ::
    ("org.apache.spark.ml.linalg.DenseMatrix", "org.apache.spark.sql.udt.MatrixUDT") ::
    ("org.apache.spark.ml.linalg.SparseMatrix", "org.apache.spark.sql.udt.MatrixUDT") ::
    Nil

  def preloadBuiltInUDT: Unit = {
    preloadedUDT.foreach { case (userClass, udtClass) =>
      UDTRegistration.register(userClass, udtClass)
    }
  }
}

