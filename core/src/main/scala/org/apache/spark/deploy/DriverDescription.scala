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

package org.apache.spark.deploy

private[spark] class DriverDescription(
    val jarUrl: String,
    val mem: Int,
    val cores: Int,
    val supervise: Boolean,
    val command: Command)
  extends Serializable {

  def copy(
      jarUrl: String = jarUrl,
      mem: Int = mem,
      cores: Int = cores,
      supervise: Boolean = supervise,
      command: Command = command): DriverDescription =
    new DriverDescription(jarUrl, mem, cores, supervise, command)

  override def toString: String = s"DriverDescription (${command.mainClass})"
}
