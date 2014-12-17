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

package org.apache.spark.sql.hive.thriftserver

private[hive] object SQLCommentUtils {

  /**
   * Filter Comments in inputed line and return a commultive string without any comments.
   * This can be used for **comment support**.
   * Three comment styles supported:
   * 1. From a '#' character to the end of the line.
   * 2. From a '--' sequence to the end of the line
   * 3. From a /* sequence to the following */ sequence, as in the C programming language.
   * This syntax allows a comment to extend over multiple lines because the beginning
   * and closing sequences need not be on the same line.
   * @param line string inputed from console
   * @param filterCommentResult the case class that encapsulates the result of filterComment
   * for making code more concise
   * @return  the case class representing result after filter comments,
   * including (cmd, isCmdEnded, commentStack)
   */
  def filterComment(line: String,
                    filterCommentResult: FilterCommentResult
                    = FilterCommentResult("", false, new scala.collection.mutable.Stack[String])
                     ): FilterCommentResult = {
    filterCommentResult.isCmdEnded = false

    if (line == null || line.trim.isEmpty) return filterCommentResult

    // define some regexes for match
    val commentMarkSeq = scala.collection.immutable.Seq(
      CommentMark("SINGLE_LINE_COMMENT_DASH", " -- ", """(.*?)--(.*)""".r),
      CommentMark("SINGLE_LINE_COMMENT_POUND", " # ", """(.*?)#(.*)""".r),
      CommentMark("MULTI_LINE_COMMENT_START", " /* ", """(.*?)/\*(.*)""".r),
      CommentMark("MULTI_LINE_COMMENT_END", " */ ", """(.*?)\*/(.*)""".r),
      CommentMark("SINGLE_QUOTATION_MARK", "'", """(.*?)'(.*)""".r),
      CommentMark("DOUBLE_QUOTATION_MARK", "\"", """(.*?)"(.*)""".r),
      CommentMark("CMD_END_MARK", ";", """(.*?);(.*)""".r)
    )

    val commentMarkMap = (for (c <- commentMarkSeq) yield (c.name -> c)).toMap

    // to get the first match in the line
    var filterMatchName = ""
    var filterMatchRegex: scala.util.matching.Regex = null
    var filterMatchIdx = -1

    commentMarkSeq.foreach(commentMark => {
      val findFirstMatch = commentMark.regex.findFirstMatchIn(line)
      if (findFirstMatch != None) {
        val matchPositonIdx = findFirstMatch.get.group(1).length
        if (matchPositonIdx < filterMatchIdx || filterMatchIdx == -1) {
          filterMatchName = commentMark.name
          filterMatchRegex = commentMark.regex
          filterMatchIdx = matchPositonIdx
        }
      }
    })

    // process the comments one by one based on head eliment in commentStack and
    // first match regex.
    val currentCommentMark = filterCommentResult.commentStack.headOption.getOrElse(null)

    if (filterMatchRegex != null) {
      val filterMatch = filterMatchRegex.findFirstMatchIn(line)
      val Array(part1, part2) = filterMatch.get.subgroups.toArray

      if (currentCommentMark == null) {
        filterMatchName match {
          case "CMD_END_MARK" =>
            filterCommentResult.cmd += part1 + commentMarkMap(filterMatchName).marker
            filterComment(part2, filterCommentResult)
            if (filterCommentResult.cmd.trim.endsWith(";")) {
              filterCommentResult.isCmdEnded = true
            }
          case "SINGLE_QUOTATION_MARK" | "DOUBLE_QUOTATION_MARK" =>
            filterCommentResult.commentStack.push(filterMatchName)
            filterCommentResult.cmd += part1 + commentMarkMap(filterMatchName).marker
            filterComment(part2, filterCommentResult)
          case "SINGLE_LINE_COMMENT_DASH" | "SINGLE_LINE_COMMENT_POUND" =>
            filterCommentResult.cmd += part1
          case "MULTI_LINE_COMMENT_START" =>
            filterCommentResult.commentStack.push(filterMatchName)
            filterCommentResult.cmd += part1
            filterComment(part2, filterCommentResult)
          case "MULTI_LINE_COMMENT_END" =>
            println(
              """WARN: found "*/", but not found matched multi line comment start marker "/*".
                |Reset your input.
              """.stripMargin)
            return FilterCommentResult("", false, new scala.collection.mutable.Stack[String])
          case _ =>
        }
      } else {
        currentCommentMark match {
          case "SINGLE_QUOTATION_MARK" | "DOUBLE_QUOTATION_MARK" =>
            filterCommentResult.cmd += part1 + commentMarkMap(filterMatchName).marker
            if (filterMatchName == currentCommentMark) {
              filterCommentResult.commentStack.pop()
            } else {
              filterCommentResult.commentStack.push(filterMatchName)
            }
            filterComment(part2, filterCommentResult)
          case "MULTI_LINE_COMMENT_START" =>
            if (filterMatchName == "MULTI_LINE_COMMENT_END") filterCommentResult.commentStack.pop()
            filterComment(part2, filterCommentResult)
        }
      }
    } else {
      // filterMatchRegex == null (not found match mark in the line)
      if (currentCommentMark == null) {
        filterCommentResult.cmd += line
      } else {
        if (currentCommentMark == "SINGLE_QUOTATION_MARK"
          || currentCommentMark == "DOUBLE_QUOTATION_MARK") {
          filterCommentResult.cmd += line
        }
      }
    }
    filterCommentResult
  }
}

/**
 * Represents result of filterComment.
 * @param cmd The commulative string for generating one cmd from multi inputs
 * @param isCmdEnded  Whether the cmd is ended.
 * @param commentStack  the context used in filterComment, stores elements that should
 *                      be considered while filtering comments, like ', ", or the begin
 *                      flag of multi line comments
 */
private[hive] case class FilterCommentResult(var cmd: String,
                               var isCmdEnded: Boolean,
                               var commentStack: scala.collection.mutable.Stack[String])

/**
 * Represents comment mark.
 * @param name the name of comment mark
 * @param marker the marker of comment mark
 * @param regex the regular expression of comment mark
 */
private[hive] case class CommentMark(name: String, marker: String, regex: scala.util.matching.Regex)
