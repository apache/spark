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
package org.apache.spark.util.expression.quantity

import scala.collection.immutable.Map

/**
 * A Utility class used for dealing in quantities of bytes
 * and converting between the various units
 * @param baseValue The number of bytes
 */
private[spark] case class ByteQuantity(baseValue: Double) {

  def this(baseValue: Double, unitOfScale: String) =
    this( baseValue * ByteQuantity.quantityScale(unitOfScale))

  def toBytes = Math.round(baseValue)

  def toKB = ByteQuantity.toKB(baseValue)
  def toMB = ByteQuantity.toMB(baseValue)
  def toGB = ByteQuantity.toGB(baseValue)
  def toTB = ByteQuantity.toTB(baseValue)
  def toPB = ByteQuantity.toPB(baseValue)
  def toEB = ByteQuantity.toEB(baseValue)

  def toKiB = ByteQuantity.toKiB(baseValue)
  def toMiB = ByteQuantity.toMiB(baseValue)
  def toGiB = ByteQuantity.toGiB(baseValue)
  def toTiB = ByteQuantity.toTiB(baseValue)
  def toPiB = ByteQuantity.toPiB(baseValue)
  def toEiB = ByteQuantity.toEiB(baseValue)

  override def hashCode = baseValue.hashCode

  /**
   * As we are dealing with bytes as the base quantity two ByteQuantities will
   * be equal is they have the same number of integral bytes
   */
  override def equals(other: Any) = other match {
    case that: ByteQuantity => math.round(this.baseValue) == math.round(that.baseValue)
    case _ => false
  }
}

private[spark] object ByteQuantity {
  val KB = "KB"
  val MB = "MB"
  val GB = "GB"
  val TB = "TB"
  val PB = "PB"
  val EB = "EB"

  val KiB = "KIB"
  val MiB = "MIB"
  val GiB = "GIB"
  val TiB = "TIB"
  val PiB = "PIB"
  val EiB = "EIB"


  val decimalPrefix = List("B",KB,MB,GB,TB,PB,EB)
    .zipWithIndex
    .map {
      case (s, i) => s->math.pow(1000, i)
    }.toMap

  val binaryPrefix = List(KiB,MiB,GiB,TiB,PiB,EiB)
    .zip(Stream from 1)
    .map {
      case (s, i) => s->math.pow(1024, i)
    }.toMap

  val jvmPrefix = List("K","M","G","T")
    .zip(Stream from 1)
    .map {
    case (s, i) => s->math.pow(1024, i)
  }.toMap

  val quantityScale: Map[String,Double] = decimalPrefix ++ binaryPrefix ++ jvmPrefix

  def apply(baseValue: Double, unitOfScale: String) =
    new ByteQuantity(baseValue,unitOfScale.toUpperCase)

  def toKB(byteQuantity: Double) = byteQuantity / ByteQuantity.quantityScale(KB)
  def toMB(byteQuantity: Double) = byteQuantity / ByteQuantity.quantityScale(MB)
  def toGB(byteQuantity: Double) = byteQuantity / ByteQuantity.quantityScale(GB)
  def toTB(byteQuantity: Double) = byteQuantity / ByteQuantity.quantityScale(TB)
  def toPB(byteQuantity: Double) = byteQuantity / ByteQuantity.quantityScale(PB)
  def toEB(byteQuantity: Double) = byteQuantity / ByteQuantity.quantityScale(EB)

  def toKiB(byteQuantity: Double) = byteQuantity / ByteQuantity.quantityScale(KiB)
  def toMiB(byteQuantity: Double) = byteQuantity / ByteQuantity.quantityScale(MiB)
  def toGiB(byteQuantity: Double) = byteQuantity / ByteQuantity.quantityScale(GiB)
  def toTiB(byteQuantity: Double) = byteQuantity / ByteQuantity.quantityScale(TiB)
  def toPiB(byteQuantity: Double) = byteQuantity / ByteQuantity.quantityScale(PiB)
  def toEiB(byteQuantity: Double) = byteQuantity / ByteQuantity.quantityScale(EiB)

}
