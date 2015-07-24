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

package org.apache.spark.sql.types

import org.apache.spark.annotation.DeveloperApi

@DeveloperApi
case class UnionType(types: Seq[DataType]) extends DataType {

  def this() = this(Nil)

  private[spark] override def asNullable: DataType = {
    new UnionType(types.map(_.asNullable).toSeq)
  }

  override def defaultSize: Int = types.map(_.defaultSize).max

  /** Readable string representation for the type. */
  override def simpleString: String = {
    val subTypes = types.map { t => s"${t.simpleString }" }
    s"union<${subTypes.mkString(",") }>"
  }

}

object UnionType extends AbstractDataType {

  private[sql] override def defaultConcreteType: DataType = apply(Nil)

  private[sql] override def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[UnionType]
  }

  override def simpleString: String = "union"

}
