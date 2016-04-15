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

import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Matrix, SparseMatrix, SparseVector, Vector}
import org.apache.spark.sql.types.UDTRegistration

/**
 * This object holds the built-in User Defined Types (UDTs) which we want to pre-load
 * into UDTRegistration.
 */
private[sql] object BuiltInUDT {
  val preloadedUDT =
    (classOf[Vector], classOf[VectorUDT]) ::
    (classOf[DenseVector], classOf[VectorUDT]) ::
    (classOf[SparseVector], classOf[VectorUDT]) ::
    (classOf[Matrix], classOf[MatrixUDT]) ::
    (classOf[DenseMatrix], classOf[MatrixUDT]) ::
    (classOf[SparseMatrix], classOf[MatrixUDT]) :: Nil

  def preloadBuiltInUDT: Unit =  {
    preloadedUDT.foreach { case (userClass, udtClass) =>
      UDTRegistration.register(userClass, udtClass)
    }
  }
}

