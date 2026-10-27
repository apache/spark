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

package org.apache.spark.sql.catalyst.expressions.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;

import org.apache.spark.sql.catalyst.json.CreateJacksonParser;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.unsafe.types.UTF8String;

public class JsonExpressionUtils {

  public static Integer lengthOfJsonArray(UTF8String json) {
    try (JsonParser jsonParser =
        CreateJacksonParser.utf8String(SharedFactory.jsonFactory(), json)) {
      if (jsonParser.nextToken() == null) {
        return null;
      }
      // Only JSON array are supported for this function.
      if (jsonParser.currentToken() != JsonToken.START_ARRAY) {
        return null;
      }
      // Parse the array to compute its length.
      int length = 0;
      // Keep traversing until the end of JSON array
      while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
        length += 1;
        // skip all the child of inner object or array
        jsonParser.skipChildren();
      }
      return length;
    } catch (IOException e) {
      return null;
    }
  }

  public static GenericArrayData jsonObjectKeys(UTF8String json) {
    try (JsonParser jsonParser =
        CreateJacksonParser.utf8String(SharedFactory.jsonFactory(), json)) {
      // return null if an empty string or any other valid JSON string is encountered
      if (jsonParser.nextToken() == null || jsonParser.currentToken() != JsonToken.START_OBJECT) {
        return null;
      }
      // Parse the JSON string to get all the keys of outermost JSON object
      List<UTF8String> arrayBufferOfKeys = new ArrayList<>();

      // traverse until the end of input and ensure it returns valid key
      while (jsonParser.nextValue() != null && jsonParser.currentName() != null) {
        // add current fieldName to the ArrayBuffer
        arrayBufferOfKeys.add(UTF8String.fromString(jsonParser.currentName()));

        // skip all the children of inner object or array
        jsonParser.skipChildren();
      }
      return new GenericArrayData(arrayBufferOfKeys.toArray());
    } catch (IOException e) {
      return null;
    }
  }

  private static final UTF8String JSON_TYPE_OBJECT = UTF8String.fromString("object");
  private static final UTF8String JSON_TYPE_ARRAY = UTF8String.fromString("array");
  private static final UTF8String JSON_TYPE_STRING = UTF8String.fromString("string");
  private static final UTF8String JSON_TYPE_NUMBER = UTF8String.fromString("number");
  private static final UTF8String JSON_TYPE_BOOLEAN = UTF8String.fromString("boolean");
  private static final UTF8String JSON_TYPE_NULL = UTF8String.fromString("null");

  public static UTF8String jsonTypeof(UTF8String json) {
    try (JsonParser jsonParser =
        CreateJacksonParser.utf8String(SharedFactory.jsonFactory(), json)) {
      JsonToken token = jsonParser.nextToken();
      if (token == null) {
        return null;
      }
      UTF8String type = switch (token) {
        case START_OBJECT -> JSON_TYPE_OBJECT;
        case START_ARRAY -> JSON_TYPE_ARRAY;
        case VALUE_STRING -> JSON_TYPE_STRING;
        case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> JSON_TYPE_NUMBER;
        case VALUE_TRUE, VALUE_FALSE -> JSON_TYPE_BOOLEAN;
        case VALUE_NULL -> JSON_TYPE_NULL;
        default -> null;
      };
      if (type == null) {
        return null;
      }
      // Consume the value so malformed input surfaces as a parse error and returns null,
      // matching json_object_keys and json_array_length.
      jsonParser.skipChildren();
      // Reject trailing content after the first value, e.g. `123 true`, so only a single
      // well-formed JSON value is accepted.
      if (jsonParser.nextToken() != null) {
        return null;
      }
      return type;
    } catch (IOException e) {
      return null;
    }
  }

  // Shape codes for the SQL `IS [NOT] JSON` predicate, kept in sync with IsJsonShape on the
  // Scala side. SHAPE_ANY covers both bare `IS JSON` and `IS JSON VALUE` (any well-formed value).
  public static final int SHAPE_ANY = 0;
  public static final int SHAPE_OBJECT = 1;
  public static final int SHAPE_ARRAY = 2;
  public static final int SHAPE_SCALAR = 3;

  // A strict JSON factory for the ANSI SQL `IS [NOT] JSON` predicate. Unlike the shared factory
  // used by the Hive-compatible JSON functions, it leaves ALLOW_SINGLE_QUOTES and
  // ALLOW_UNESCAPED_CONTROL_CHARS disabled, so only well-formed ANSI JSON is accepted
  // (e.g. `{'a':1}` with single quotes is rejected).
  private static final JsonFactory STRICT_JSON_FACTORY = new JsonFactory();

  /**
   * Evaluates the SQL `IS JSON` predicate for a single, already-evaluated operand: returns whether
   * {@code json} is a well-formed ANSI JSON value of the requested {@code shape}. The operand is
   * read exactly once here, so a non-deterministic input is evaluated a single time (unlike a
   * plan that references the child in more than one place). NULL handling (NULL input yields a
   * NULL result) is left to the calling expression; this method is only invoked for non-null
   * input. Malformed input yields false, but input that exceeds one of Jackson's default
   * {@code StreamReadConstraints} (nesting depth, number length, string length, or field name
   * length) cannot be classified and raises IS_JSON_INPUT_LIMIT_EXCEEDED, at the same limits
   * where {@code parse_json} fails.
   */
  public static boolean isJson(UTF8String json, int shape) {
    // The parser decodes the bytes through a UTF-8 reader that silently replaces malformed
    // sequences with U+FFFD, so check the raw bytes first: input that is not valid UTF-8
    // (e.g. a lone continuation byte, or an encoded surrogate) is not well-formed JSON text.
    if (!json.isValid()) {
      return false;
    }
    try (JsonParser jsonParser =
        CreateJacksonParser.utf8String(STRICT_JSON_FACTORY, json)) {
      JsonToken token = jsonParser.nextToken();
      if (token == null) {
        // Empty input is not a JSON value.
        return false;
      }
      boolean matchesShape = switch (shape) {
        case SHAPE_OBJECT -> token == JsonToken.START_OBJECT;
        case SHAPE_ARRAY -> token == JsonToken.START_ARRAY;
        case SHAPE_SCALAR -> switch (token) {
          case VALUE_STRING, VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT,
               VALUE_TRUE, VALUE_FALSE, VALUE_NULL -> true;
          default -> false;
        };
        // SHAPE_ANY: any well-formed JSON value (object, array, or scalar).
        default -> switch (token) {
          case START_OBJECT, START_ARRAY, VALUE_STRING, VALUE_NUMBER_INT,
               VALUE_NUMBER_FLOAT, VALUE_TRUE, VALUE_FALSE, VALUE_NULL -> true;
          default -> false;
        };
      };
      if (!matchesShape) {
        return false;
      }
      // Walk the whole value token by token (rather than `skipChildren`, which does not look at
      // string contents) so that malformed input (e.g. `{`, `[1,`) surfaces as a parse error
      // and every string value and field name can be checked for unpaired surrogates.
      int depth = 0;
      do {
        switch (token) {
          case START_OBJECT, START_ARRAY -> depth++;
          case END_OBJECT, END_ARRAY -> depth--;
          case FIELD_NAME, VALUE_STRING -> {
            if (hasUnpairedSurrogate(jsonParser.getText())) {
              return false;
            }
          }
          default -> { }
        }
      } while (depth > 0 && (token = jsonParser.nextToken()) != null);
      if (depth > 0) {
        return false;
      }
      // Reject trailing content after the first value, e.g. `1 2` or `{} []`, so only a single
      // well-formed JSON value is accepted.
      return jsonParser.nextToken() == null;
    } catch (StreamConstraintsException e) {
      throw QueryExecutionErrors.isJsonInputLimitExceededError(describeLimit(e), json, e);
    } catch (IOException e) {
      return false;
    }
  }

  // Describes the parser limit that `e` reports, for IS_JSON_INPUT_LIMIT_EXCEEDED. Jackson does
  // not expose which limit was exceeded, so this matches the message prefixes used by
  // StreamReadConstraints, and lists every limit if the message is not recognized.
  private static String describeLimit(StreamConstraintsException e) {
    StreamReadConstraints limits = STRICT_JSON_FACTORY.streamReadConstraints();
    String message = String.valueOf(e.getMessage());
    if (message.startsWith("Document nesting depth")) {
      return "the nesting depth of objects and arrays exceeds the maximum of " +
          limits.getMaxNestingDepth();
    } else if (message.startsWith("Number value length")) {
      return "a number is longer than the maximum of " + limits.getMaxNumberLength() +
          " characters";
    } else if (message.startsWith("String value length")) {
      return "a string value is longer than the maximum of " + limits.getMaxStringLength() +
          " characters";
    } else if (message.startsWith("Name length")) {
      return "a field name is longer than the maximum of " + limits.getMaxNameLength() +
          " characters";
    } else {
      return String.format(
          "the limits are a nesting depth of %d and lengths of %d characters for numbers, " +
              "%d for string values, and %d for field names",
          limits.getMaxNestingDepth(), limits.getMaxNumberLength(), limits.getMaxStringLength(),
          limits.getMaxNameLength());
    }
  }

  // Returns whether `str` contains an unpaired UTF-16 surrogate code unit. Jackson accepts an
  // escaped lone surrogate (e.g. a high surrogate escape with no low surrogate after it), and so
  // does the RFC 8259 grammar (section 8.2), but such a string is not well-formed Unicode.
  // Rejecting it is a deliberate rule stricter than RFC 8259, matching I-JSON (RFC 7493
  // section 2.1) and the strict check in VariantBuilder.checkValidUnicodeString.
  private static boolean hasUnpairedSurrogate(String str) {
    int len = str.length();
    for (int i = 0; i < len; ++i) {
      char c = str.charAt(i);
      if (Character.isHighSurrogate(c)) {
        if (i + 1 >= len || !Character.isLowSurrogate(str.charAt(i + 1))) {
          return true;
        }
        ++i;
      } else if (Character.isLowSurrogate(c)) {
        return true;
      }
    }
    return false;
  }
}
