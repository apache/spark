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

package org.apache.spark.sql;

import java.io.IOException;
import java.io.StringReader;
import java.util.Locale;
import java.util.Vector;
import org.apache.spark.SparkException;

/**
 * Methods for handling SQL identifiers. These methods were cribbed
 * from org.apache.derby.iapi.util.IdUtil and
 * org.apache.derby.iapi.util.StringUtil.
 */
public class SqlIdentifierUtil {

  /**
   * Quote a string so that it can be used as an identifier or a string
   * literal in SQL statements. Identifiers are surrounded by double quotes
   * and string literals are surrounded by single quotes. If the string
   * contains quote characters, they are escaped.
   *
   * @param source the string to quote
   * @param quote  the character to quote the string with (' or &quot;)
   * @return a string quoted with the specified quote character
   */
  public static String quoteString(String source, char quote) {
    // Normally, the quoted string is two characters longer than the source
    // string (because of start quote and end quote).
    StringBuffer quoted = new StringBuffer(source.length() + 2);
    quoted.append(quote);
    for (int i = 0; i < source.length(); i++) {
      char c = source.charAt(i);
      // if the character is a quote, escape it with an extra quote
      if (c == quote) quoted.append(quote);
      quoted.append(c);
    }
    quoted.append(quote);
    return quoted.toString();
  }

  /**
   * Parse a multi-part (dot separated) SQL identifier from the
   * String provided. Raise an excepion
   * if the string does not contain valid SQL indentifiers.
   * The returned String array contains the normalized form of the
   * identifiers.
   *
   * @param s        The string to be parsed
   * @param quoteCharacter The character which frames a delimited id (e.g., " or `)
   * @param upperCaseIdentifiers True if SQL ids are normalized to upper case.
   * @return An array of strings made by breaking the input string at its dots, '.'.
   * @throws SparkException Invalid SQL identifier.
   */
  public static String[] parseMultiPartSQLIdentifier(
      String s,
      char quoteCharacter,
      boolean upperCaseIdentifiers)
      throws SparkException {
    StringReader r = new StringReader(s);
    String[] qName = parseMultiPartSQLIdentifier(
        s,
        r,
        quoteCharacter,
        upperCaseIdentifiers);
    verifyEmpty(s, r);
    return qName;
  }

  /**
   * Parse a multi-part (dot separated) SQL identifier from the
   * String provided. Raise an excepion
   * if the string does not contain valid SQL indentifiers.
   * The returned String array contains the normalized form of the
   * identifiers.
   *
   * @param orig The full text being parsed
   * @param r        The multi-part identifier to be parsed
   * @param quoteCharacter The character which frames a delimited id (e.g., " or `)
   * @param upperCaseIdentifiers True if SQL ids are normalized to upper case.
   * @return An array of strings made by breaking the input string at its dots, '.'.
   * @throws SparkException Invalid SQL identifier.
   */
  private static String[] parseMultiPartSQLIdentifier(
      String orig,
      StringReader r,
      char quoteCharacter,
      boolean upperCaseIdentifiers)
      throws SparkException {
    Vector<String> v = new Vector<String>();
    while (true) {
        String thisId = parseId(orig, r, quoteCharacter, upperCaseIdentifiers);
      v.add(thisId);
      int dot;

      try {
        r.mark(0);
        dot = r.read();
        if (dot != '.') {
          if (dot != -1) r.reset();
          break;
        }
      } catch (IOException ioe) {
        throw parseError(orig, ioe);
      }
    }
    String[] result = new String[v.size()];
    v.copyInto(result);
    return result;
  }

  /**
   * Read an id from the StringReader provided.
   * <p>
   * <p>
   * Raise an exception if the first thing in the StringReader
   * is not a valid id.
   * </P>
   *
   * @param orig The full text being parsed
   * @param r        The multi-part identifier to be parsed
   * @param quoteCharacter The character which frames a delimited id (e.g., " or `)
   * @param upperCaseIdentifiers True if SQL ids are normalized to upper case.
   * @throws SparkException Invalid SQL identifier.
   */
  private static String parseId(
      String orig,
      StringReader r,
      char quoteCharacter,
      boolean upperCaseIdentifiers)
      throws SparkException {
    try {
      r.mark(0);
      int c = r.read();
      if (c == -1) {  //id can't be 0-length
        throw parseError(orig, null);
      }
      r.reset();
      if (c == quoteCharacter) {
        return parseQId(orig, r, quoteCharacter);
      } else {
        return parseUnQId(orig, r, upperCaseIdentifiers);
      }
    } catch (IOException ioe) {
      throw parseError(orig, ioe);
    }
  }

  /**
   * Parse a regular identifier (unquoted) returning returning either
   * the value of the identifier or a delimited identifier. Ensures
   * that all characters in the identifer are valid for a regular identifier.
   *
   * @param orig The full text being parsed
   * @param r Regular identifier to parse.
   * @param upperCaseIdentifiers True if SQL ids are normalized to upper case.
   * @return the value of the identifer or a delimited identifier
   * @throws SparkException Error accessing value
   */
  private static String parseUnQId(
      String orig,
      StringReader r,
      boolean upperCaseIdentifiers)
    throws SparkException {
    StringBuffer b = new StringBuffer();
    int c;
    boolean first;

    try {
      for (first = true; ; first = false) {
        r.mark(0);
        if (idChar(first, c = r.read())) {
          b.append((char) c);
        } else {
          break;
        }
      }

      if (c != -1) {
        r.reset();
      }
    } catch (IOException ioe) {
      throw parseError(orig, ioe);
    }

    String id = b.toString();

    return adjustCase(id, upperCaseIdentifiers);
  }

  private static boolean idChar(boolean first, int c) {
    if (((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) ||
        (!first && (c >= '0' && c <= '9')) || (!first && c == '_')) {
      return true;
    } else if (Character.isLetter((char) c)) {
      return true;
    } else if (!first && Character.isDigit((char) c)) {
      return true;
    }

    return false;
  }

  /**
   * Adjust the case of an unquoted identifier.
   * Always use the java.util.ENGLISH locale
   *
   * @param s string to uppercase
   * @param upperCaseIdentifiers True if SQL ids are normalized to upper case.
   * @return uppercased string
   */
  private static String adjustCase(String s, boolean upperCaseIdentifiers) {
    if ( upperCaseIdentifiers) {
      return s.toUpperCase(Locale.ENGLISH);
    }
    else {
      return s.toLowerCase(Locale.ENGLISH);
    }
  }

  /**
   * Parse a delimited (quoted) identifier returning either
   * the value of the identifier or a delimited identifier.
   *
   * @param orig The full text being parsed
   * @param r Quoted identifier to parse.
   * @return the value of the identifer or a delimited identifier
   * @throws SparkException Error parsing identifier.
   */
  private static String parseQId(
      String orig,
      StringReader r,
      char quoteCharacter)
      throws SparkException {
    StringBuffer b = new StringBuffer();
    
    try {
      int c = r.read();
      if (c != quoteCharacter) {
        throw parseError(orig, null);
      }

      while (true) {
        c = r.read();
        if (c == quoteCharacter) {
          r.mark(0);
          int c2 = r.read();
          if (c2 != quoteCharacter) {
            if (c2 != -1) {
                r.reset();
            }
            break;
          }
        } else if (c == -1) {
          throw parseError(orig, null);
        }

        b.append((char) c);
      }
    } catch (IOException ioe) {
      throw parseError(orig, ioe);
    }

    if (b.length() == 0) { //id can't be 0-length
      throw parseError(orig, null);
    }

    return b.toString();
  }

  /**
   * Verify the read is empty (no more characters in its stream).
   *
   * @param orig The full text being parsed
   * @param r
   * @throws SparkException
   */
  private static void verifyEmpty(String orig, java.io.Reader r)
      throws SparkException {
    try {
      if (r.read() != -1) {
        throw parseError(orig, null);
      }
    } catch (IOException ioe) {
      throw parseError(orig, ioe);
    }
  }

  /**
   * Create a parsing exception.
   *
   * @param orig The full text being parsed
   * @param cause Optional original exception
   * @return A SparkException describing a parsing error.
   */
  private static SparkException parseError(String orig, Exception cause) {
    String message = "Error parsing SQL identifier: " + orig;

    if (cause != null) {
      return new SparkException(message);
    } else {
      return new SparkException(message, cause);
    }
  }

}
