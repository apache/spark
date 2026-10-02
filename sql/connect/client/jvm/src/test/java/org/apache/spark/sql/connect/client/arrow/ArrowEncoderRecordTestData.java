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
package org.apache.spark.sql.connect.client.arrow;

import java.util.List;
import java.util.Map;

/**
 * Java records used by ArrowEncoderSuite. Records cannot be declared in Scala.
 */
public class ArrowEncoderRecordTestData {

  public record SimpleRecord(int id, String name, Double score) {}

  public record Address(String city, String zip) {}

  public record Person(String name, long age, Address address, List<String> tags,
      Map<String, Integer> counts) {}

  public record Box<T>(T value) {}

  public record NumberBox<T extends Number>(T value) {}

  public record ComparableBox<T extends Comparable<T>>(T value) {}

  public record BoxHolder(Box<String> box, NumberBox<Integer> numberBox,
      ComparableBox<Integer> comparableBox) {}
}
