/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonUtils {
  protected static ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static ObjectMapper getMapper() {
    return mapper;
  }

  public static String serialize(Object obj) {
    if (obj == null) {
      return "";
    }
    try {
      return mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(String.format("Failed to serialize %s (%s)", obj, obj.getClass()),
          e);
    }
  }

  public static <T> T deserialize(String content, Class<T> valueType) {
    try {
      return mapper.readValue(content, valueType);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to deserialize %s from json %s", valueType, content), e);
    }
  }

}
