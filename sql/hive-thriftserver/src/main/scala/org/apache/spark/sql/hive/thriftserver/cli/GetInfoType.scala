/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli

import org.apache.spark.service.cli.thrift.TGetInfoType

trait GetInfoType {
  def toTGetInfoType: TGetInfoType = null
}

object GetInfoType {

  case object CLI_DBMS_NAME extends GetInfoType {
    override val toTGetInfoType: TGetInfoType = TGetInfoType.CLI_DBMS_NAME
  }

  case object CLI_SERVER_NAME extends GetInfoType {
    override val toTGetInfoType: TGetInfoType = TGetInfoType.CLI_SERVER_NAME
  }

  case object CLI_DBMS_VER extends GetInfoType {
    override val toTGetInfoType: TGetInfoType = TGetInfoType.CLI_DBMS_VER
  }

  def getGetInfoType(tGetInfoType: TGetInfoType): GetInfoType = tGetInfoType match {
    case CLI_DBMS_NAME.toTGetInfoType => CLI_DBMS_NAME
    case CLI_SERVER_NAME.toTGetInfoType => CLI_SERVER_NAME
    case CLI_DBMS_VER.toTGetInfoType => CLI_DBMS_VER
    case _ =>
      throw new IllegalArgumentException("Unrecognized Thrift TGetInfoType value: " + tGetInfoType)
  }
}