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
package org.apache.spark

import scala.collection.mutable.{ListBuffer, StringBuilder}

class StringSubstitutor(
    resolver: Map[String, Any],
    prefix: String = "${",
    suffix: String = "}",
    escape: Char = '$',
    valueDelimiter: String = ":-",
    preserveEscapes: Boolean = true,
    enableSubstitutionInVariables: Boolean = false,
    enableUndefinedVariableException: Boolean = true) {

  private val prefixLen = prefix.length
  private val suffixLen = suffix.length

  def replace(source: String): String = {
    if (source == null) return null
    val buf = new StringBuilder(source)
    if (!substitute(buf, 0, buf.length, ListBuffer.empty[String])) {
      source
    } else {
      buf.toString
    }
  }

  private def substitute(
      buf: StringBuilder,
      offset: Int,
      length: Int,
      priorVariables: ListBuffer[String]): Boolean = {
    var altered = false
    var currentPos = offset
    val endPos = offset + length

    while (currentPos < endPos) {
      // Find prefix
      val prefixPos = buf.indexOf(prefix, currentPos)
      if (prefixPos < 0 || prefixPos >= endPos) {
        return altered
      }

      if (prefixPos > 0 && buf.charAt(prefixPos - 1) == escape) {
        if (preserveEscapes) {
          currentPos = prefixPos + prefixLen
        } else {
          buf.deleteCharAt(prefixPos - 1)
          altered = true
          currentPos = prefixPos - 1 + prefixLen // adjust position
        }
      } else {
        // Find suffix
        val suffixPos = buf.indexOf(suffix, prefixPos + prefixLen)
        if (suffixPos < 0) {
          currentPos = prefixPos + prefixLen
        } else {
          // Extract variable name and default
          val varFull = buf.substring(prefixPos + prefixLen, suffixPos)
          val index = varFull.indexOf(valueDelimiter)
          val (varName, defaultValue) = if (index >= 0) {
            (varFull.substring(0, index), Some(varFull.substring(index + valueDelimiter.length)))
          } else {
            (varFull, None)
          }

          // Check for cycle
          if (priorVariables.contains(varName)) {
            throw new IllegalStateException(
              s"Infinite loop in property interpolation of $varName")
          }

          resolver.get(varName) match {
            case Some(value) =>
              var replacement = value.toString
              if (enableSubstitutionInVariables) {
                val newPrior = priorVariables.clone() += varName
                val tempBuf = new StringBuilder(replacement)
                if (substitute(tempBuf, 0, tempBuf.length, newPrior)) {
                  replacement = tempBuf.toString
                }
              }
              buf.replace(prefixPos, suffixPos + suffixLen, replacement)
              // Adjust positions and recurse on the changed part
              val changeInLength = replacement.length - (suffixPos + suffixLen - prefixPos)
              val newLength = length + changeInLength
              substitute(buf, prefixPos + replacement.length, newLength, priorVariables)
              return true // restart scan after change

            case None =>
              defaultValue match {
                case Some(value) =>
                  buf.replace(prefixPos, suffixPos + suffixLen, value)
                  val changeInLength = value.length - (suffixPos + suffixLen - prefixPos)
                  val newLength = length + changeInLength
                  substitute(buf, prefixPos + value.length, length, priorVariables)
                  return true
                case None =>
                  if (enableUndefinedVariableException) {
                    throw new IllegalArgumentException(s"Undefined variable: $varName")
                  }
              }
          }
          currentPos = suffixPos + suffixLen
        }
      }
    }
    altered
  }
}
