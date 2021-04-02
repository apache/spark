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
package org.apache.spark.sql.errors

import org.apache.spark.errors.ErrorCode

object catalystErrors {
  case object INVALID_PARAMETER_VALUE extends ErrorCode(200001, "22023")
  case object DIVISION_BY_ZERO extends ErrorCode(200002, "22012")
  case object INVALID_DATETIME_FORMAT extends ErrorCode(200003, "22007")
  case object INVALID_INTERVAL_FORMAT extends ErrorCode(200004, "22006")
  case object NULL_VALUE_NOT_ALLOWED extends ErrorCode(200005, "22004")
  case object NUMERIC_VALUE_OUT_OF_RANGE extends ErrorCode(200006, "22003")
  case object SUBSTRING_ERROR extends ErrorCode(200007, "22011")
  case object TRIM_ERROR extends ErrorCode(200008, "22027")
}
