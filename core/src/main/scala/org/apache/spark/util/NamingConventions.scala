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

package org.apache.spark.util

/**
 * all utilities related to naming conventions 
 */
private[spark] object NamingConventions {
  
  /**
   * Lower camelCase which convert the phrases into camelCase style with the first letter lowercase
   */
  def lowerCamelCaseConversion(phrases: Seq[String]): Seq[String] = {
    var first = true
    
    for (elem <- phrases) yield {
      if (first) {
        first = false
        elem
      }
      else {
        elem.capitalize
      }
    }
  }
  
  /**
   * The standard camelCase style
   */
  def camelCaseConversion(phrases: Seq[String]): Seq[String] = {
    phrases.map(_.capitalize)
  }
  
  def noConversion = { x: Seq[String] => x }
  
  /**
   * Concatenate the words using certain naming style.
   * The default style is lowerCamelCase with empty connector.
   */
  def makeIdentifier(phrases: Seq[String], namingConversion: (Seq[String]) => Seq[String] = lowerCamelCaseConversion) (implicit connector: String = "" ): String = {
    namingConversion(phrases.filter(_.nonEmpty)).mkString(connector)
  }
  
  def makeMetricName(phrases: String*): String = {
    makeIdentifier(phrases, noConversion)("_")
  }
}