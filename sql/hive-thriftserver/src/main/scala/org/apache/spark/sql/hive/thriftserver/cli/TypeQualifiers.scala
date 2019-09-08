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

import scala.collection.JavaConverters._

import org.apache.spark.service.cli.thrift.{TCLIServiceConstants, TTypeQualifiers, TTypeQualifierValue}
import org.apache.spark.sql.types.{DataType, DecimalType}

class TypeQualifiers private() {
  private[this] var precision: Option[Int] = None
  private[this] var scale: Option[Int] = None

  private def setPrecision(precision: Int): Unit = {
    this.precision = Some(precision)
  }

  private def setScale(scale: Int): Unit = {
    this.scale = Some(scale)
  }

  def toTTypeQualifiers: TTypeQualifiers = new TTypeQualifiers(
    (precision.map(TTypeQualifierValue.i32Value).map(TCLIServiceConstants.PRECISION -> _) ++
      scale.map(TTypeQualifierValue.i32Value).map(TCLIServiceConstants.SCALE -> _)).toMap.asJava)
}

object TypeQualifiers {
  def fromTypeInfo(typ: DataType): TypeQualifiers = {
    val result = new TypeQualifiers
    typ match {
      case decimalType: DecimalType =>
        result.setScale(decimalType.scale)
        result.setPrecision(decimalType.precision)
      case _ =>
    }
    result
  }
}
