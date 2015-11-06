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

package org.apache.spark.sql

import java.util.Locale
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException

/**
 * A three part table identifier. The first two parts can be null.
 *
 * @param database The database name.
 * @param schema The schema name.
 * @param table The table name.
 */
case class TableId(database: String, schema: String, table: String)

/**
 * Utility methods for SQL identifiers. These methods were loosely
 * translated from org.apache.derby.iapi.util.IdUtil and
 * org.apache.derby.iapi.util.StringUtil.
 */
object SqlIdUtil {

  private val OneQuote = """""""
  private val TwoQuotes = """"""""
  private val DefaultQuote = '"'

  // Regular expression defining one id in a dot-separated SQL identifier chain
  private val OneIdString =
    "(\\s)*((" +                     // leading spaces ok
    """\p{Alpha}(\p{Alnum}|_)*""" +  // regular identifier (no quotes)
    ")|(" +                          // or
    """"(""|[^"])+"""" +             // delimited identifier (quoted)
    "))(\\s)*"                       // trailing spaces ok

  /**
   * Quote a string so that it can be used as an identifier or a string
   * literal in SQL statements. Identifiers are usually surrounded by double quotes
   * and string literals are surrounded by single quotes. If the string
   * contains quote characters, they are escaped.
   *
   * @param source the string to quote
   * @param quote  the framing quote character (e.g.: ', ", `)
   * @return a string quoted with the indicated quote character
   */
  def quoteString(source: String, quote: Char): String = {
    // Normally, the quoted string is two characters longer than the source
    // string (because of start quote and end quote).
    val quoted = new StringBuilder(source.length() + 2)

    quoted.append(quote)
    for (ch <- source) {
      quoted.append(ch)
      if (ch == quote) quoted.append(quote)
    }
    quoted.append(quote)
    quoted.toString()
  }

  /** Parse a user-supplied object id of the form
    * [[database.]schema.]objectName
    * into a TableIdentifier(database, schema, objectName).
    * The database and schema names may be empty. The caller
    * must supply the database-specific quote character which is used
    * to frame delimited ids. For most databases this is the "
    * character. For Hive, this is the ` character. The caller must
    * specify whether the database uppercases or lowercases
    * unquoted identifiers when they are stored in its metadata
    * catalogs.
    *
    * The fields of the TableIdentifier are normalized to the case
    * convention used by the database's catalogs. So for a database
    * which uses " for quoted identifiers and which uppercases
    * ids in its metadata catalogs, the string
    *
    *    "foo".bar
    *
    * would result in
    *
    *    TableIdentifier( null, foo, BAR )
    *
    * @param rawName The user-supplied name.
    * @param quote The db-specific character which frames delimited ids.
    * @param upperCase True if the db uppercases un-delimited ids.
    */
  def parseSqlIds(
      rawName: String,
      quote: Char,
      upperCase: Boolean): TableId = {
    val parsed = parseMultiPartSqlIdentifier(rawName,
       quote, upperCase)

    parsed.length match {
      case 1 => TableId(null, null, parsed(0))
      case 2 => TableId(null, parsed(0), parsed(1))
      case 3 => TableId(parsed(0), parsed(1), parsed(2))
      case _ => throw new Exception("Unparsable object id: " + rawName)
    }
  }

  /**
   * Parse a multi-part (dot separated) chain of SQL identifiers from the
   * String provided. Raise an excepion
   * if the string does not contain valid SQL indentifiers.
   * The returned String array contains the normalized form of the
   * identifiers.
   *
   * @param rawName The string to be parsed
   * @param quote The character which frames a delimited id (e.g., " or `)
   * @param upperCase True if SQL ids are normalized to upper case.
   * @return An array of strings made by breaking the input string at its dots, '.'.
   * @throws SparkException Invalid SQL identifier.
   */
  private def parseMultiPartSqlIdentifier(
      rawName: String,
      quote: Char,
      upperCase: Boolean): ArrayBuffer[String] = {

    // construct the regex, accounting for the caller-supplied quote character
    var regexString = OneIdString
    if (quote != DefaultQuote)
    {
      regexString = regexString.replace(DefaultQuote, quote)
    }
    val oneIdRegex = regexString.r

    //
    // Loop through the raw string, one identifier at a time.
    // Discard spaces around the identifiers. Discard
    // the dots which separate one identifier from the next.
    //
    var result = ArrayBuffer[String]()
    var keepGoing = true
    var remainingString = rawName
    while (keepGoing)
    {
      oneIdRegex.findPrefixOf(remainingString) match {

        case Some(paddedId) => {
          val paddedIdLength = paddedId.length
          result.append(normalize(paddedId.trim, quote, upperCase))
          if (remainingString.length == paddedIdLength) {
            keepGoing = false    // we're done. hooray.
          }
          else if (remainingString.charAt(paddedIdLength) == '.') {
            // chop off the old identifier and the dot separator.
            // continue looking for more ids in the rest of the string.
            remainingString = remainingString.substring(paddedIdLength + 1)
          }
          else {
            throw parseError(rawName)
          }
        }

        case _ => {
          throw parseError(rawName)
        }
      } // end matching an id

    }  // end of loop through ids

    result
  }

  /**
   * Normalize a SQL identifier to the case used by the target
   * database's metadata catalogs.
   *
   * @param rawName The string to be normalized (may be framed by quotes)
   * @param quote The character which frames a delimited id (e.g., " or `)
   * @param upperCase True if SQL ids are normalized to upper case.
   * @return An array of strings made by breaking the input string at its dots, '.'.
   */
  private def normalize(rawName: String, quote: Char, upperCase: Boolean): String = {

    // regular id
    if (rawName.charAt(0) != quote) adjustCase(rawName, upperCase)
    // delimited id
    else stripQuotes(rawName, quote)
  }

  /**
   * Adjust the case of an unquoted identifier to the case convention
   * used by the metadata catalogs of the target database.
   * Always use the java.util.ENGLISH locale.
   *
   * @param rawName string to uppercase
   * @param upperCase True if SQL ids are normalized to upper case.
   * @return The properly cased string.
   */
  private def adjustCase(rawName: String, upperCase: Boolean): String = {
    if (upperCase) rawName.toUpperCase(Locale.ENGLISH)
    else rawName.toLowerCase(Locale.ENGLISH)
  }

  /**
   * Strip framing quotes from a delimited id and un-escape interior quotes.
   *
   * @param rawName string to uppercase
   * @param quote the database-specific quote character.
   * @return The properly cased string.
   */
  private def stripQuotes(rawName: String, quote: Char): String = {
    var oneQuote = OneQuote
    var twoQuotes = TwoQuotes
    if ( quote != DefaultQuote)
    {
      val oneQuote = OneQuote.replace(DefaultQuote, quote)
      val twoQuotes = TwoQuotes.replace(DefaultQuote, quote)
    }
    rawName.substring(1, rawName.length - 1).replace(twoQuotes, oneQuote)
  }

  /**
   * Create a parsing exception.
   *
   * @param orig The full text being parsed
   * @return A SparkException describing a parsing error.
   */
  private def parseError(orig: String): SparkException = {
    new SparkException("Error parsing SQL identifier: " + orig)
  }

}
