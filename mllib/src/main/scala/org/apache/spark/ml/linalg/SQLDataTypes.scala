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

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.sql.types.DataType

/**
 * :: DeveloperApi ::
 * SQL data types for vectors and matrices.
 */
@Since("2.0.0")
@DeveloperApi
object SQLDataTypes {

  /** Data type for [[Vector]]. */
  val VectorType: DataType = new VectorUDT

  /** Data type for [[Matrix]]. */
  val MatrixType: DataType = new MatrixUDT
}
