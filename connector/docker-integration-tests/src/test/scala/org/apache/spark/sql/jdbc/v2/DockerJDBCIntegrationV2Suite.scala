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

package org.apache.spark.sql.jdbc.v2

import java.sql.Connection

import org.apache.spark.sql.jdbc.DockerJDBCIntegrationSuite

abstract class DockerJDBCIntegrationV2Suite extends DockerJDBCIntegrationSuite {

  /**
   * Prepare databases and tables for testing.
   */
  override def dataPreparation(connection: Connection): Unit = {
    tablePreparation(connection)
    connection.prepareStatement("INSERT INTO employee VALUES (1, 'amy', 10000, 1000)")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO employee VALUES (2, 'alex', 12000, 1200)")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO employee VALUES (1, 'cathy', 9000, 1200)")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO employee VALUES (2, 'david', 10000, 1300)")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO employee VALUES (6, 'jen', 12000, 1200)")
      .executeUpdate()
  }

  def tablePreparation(connection: Connection): Unit
}
