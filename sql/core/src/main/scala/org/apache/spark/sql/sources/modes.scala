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
package org.apache.spark.sql.sources

/**
  * SaveMode is used to specify the expected behavior of saving a DataFrame to a data source.
  */
abstract class SaveMode {

}

/**
  * Append mode means that when saving a DataFrame to a data source, if data already exists,
  * contents of the DataFrame are expected to be appended to existing data.
  *
  * Please use the singleton [[SaveModes.Append]].
  */
case object Append extends SaveMode

/**
 * Overwrite mode means that when saving a DataFrame to a data source, if data already exists,
 * existing data is expected to be overwritten by the contents of the DataFrame.
 *
 * Please use the singleton [[SaveModes.Overwrite]].
 */
case object Overwrite extends SaveMode

/**
 * ErrorIfExists mode means that when saving a DataFrame to a data source, if data already exists,
 * an exception is expected to be thrown.
 *
 * Please use the singleton [[SaveModes.ErrorIfExists]].
 */
case object ErrorIfExists extends SaveMode
