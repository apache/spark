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

import scala.Option;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.spark.sql.catalyst.expressions.SharedFactory;
import org.apache.spark.sql.catalyst.json.CreateJacksonParser;
import org.apache.spark.sql.catalyst.json.JSONOptions;
import org.apache.spark.sql.catalyst.json.JsonInferSchema;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class JsonExpressionUtils {

  public static Integer lengthOfJsonArray(UTF8String json) {
    // return null for null input
    if (json == null) {
      return null;
    }
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
    // return null for `NULL` input
    if (json == null) {
      return null;
    }
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

  public static UTF8String schemaOfJson(
      JsonFactory jsonFactory,
      JSONOptions jsonOptions,
      JsonInferSchema jsonInferSchema,
      UTF8String json) {
    DataType schema;
    try (JsonParser jsonParser = CreateJacksonParser.utf8String(jsonFactory, json)) {
      jsonParser.nextToken();
      // To match with schema inference from JSON datasource.
      DataType inferSchema = jsonInferSchema.inferField(jsonParser);
      if (inferSchema instanceof StructType) {
        Option<DataType> canonicalType = jsonInferSchema.canonicalizeType(inferSchema, jsonOptions);
        schema = canonicalType.isDefined() ?
          canonicalType.get() : new StructType(new StructField[0]);
      } else if (inferSchema instanceof ArrayType at && at.elementType() instanceof StructType et) {
        Option<DataType> canonicalType = jsonInferSchema.canonicalizeType(et, jsonOptions)
          .map(dt -> ArrayType.apply(dt, at.containsNull()));
        schema = canonicalType.isDefined() ? canonicalType.get() :
          ArrayType.apply(new StructType(new StructField[0]), at.containsNull());
      } else {
        Option<DataType> canonicalType = jsonInferSchema.canonicalizeType(inferSchema, jsonOptions);
        schema = canonicalType.isDefined() ?
          canonicalType.get() : SQLConf.get().defaultStringType();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return UTF8String.fromString(schema.sql());
  }
}
