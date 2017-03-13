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
 *
 *
 *1) Download jFuzzyLogic_core.jar from http://jfuzzylogic.sourceforge.net/html/index.html
 *
 *2) Install jar into maven repository using this command:
 *
 *mvn install:install-file \
 *  -Dfile=jFuzzyLogic_core.jar \
 *  -DgroupId=net.sourceforge.jFuzzyLogic \
 *  -DartifactId=jFuzzyLogicCore \
 *  -Dversion=1.0 \
 *  -Dpackaging=jar
 *3) Create a eclipse project sbt eclipse
 *
 */

package org.apache.spark.mllib.fuzzylogic

object fuzzylogic {
  
  private val MIN_VALUE = 0.0
  
  private val MAX_VALUE = 1.0
  
  val MIN = new fuzzylogic(MIN_VALUE)

  val MAX = new fuzzylogic(MAX_VALUE)

  def apply() = MIN

  def apply(v: Double) = new fuzzylogic(v)
}

class fuzzylogic private (v: Double) extends Number {

  require(v >= fuzzylogic.MIN_VALUE && v <= fuzzylogic.MAX_VALUE)

  val value = v

  def intValue() = value.intValue()

  def longValue() = value.longValue()

  def floatValue() = value.floatValue()

  def doubleValue() = value.doubleValue()

  override def toString(): String = value.toString()

  override def equals(other: Any): Boolean =
    other match {
      case that: fuzzylogic =>
        (that canEqual this) &&
          value == that.doubleValue()

      case _ => false
    }

  override def hashCode(): Int = value.hashCode()

  def canEqual(other: Any): Boolean = other.isInstanceOf[fuzzylogic]

  def &(other: fuzzylogic): fuzzylogic = if (other.doubleValue() < value) other else this

  def |(other: fuzzylogic): fuzzylogic = if (other.doubleValue() > value) other else this
  
  def unary_! = fuzzylogic(fuzzylogic.MAX_VALUE - value)
  
  def not() = unary_!
    
}