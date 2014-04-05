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

/**
 * Class that represents the singular value decomposition of a matrix
 *
 * @param U such that A = USV^T is a TallSkinnyDenseMatrix
 * @param S such that A = USV^T is a simple double array
 * @param V such that A = USV^T, V is a 2d array matrix that holds
 *          singular vectors in columns. Columns are inner arrays
 *          i.e. V(i)(j) is standard math notation V_{ij}
 */
case class TallSkinnyMatrixSVD(val U: TallSkinnyDenseMatrix,
                               val S: Array[Double],
                               val V: Array[Array[Double]])
