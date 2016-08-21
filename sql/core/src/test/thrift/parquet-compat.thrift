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

namespace java org.apache.spark.sql.execution.datasources.parquet.test.thrift

enum Suit {
    SPADES,
    HEARTS,
    DIAMONDS,
    CLUBS
}

struct Nested {
    1: required list<i32> nestedIntsColumn;
    2: required string nestedStringColumn;
}

/**
 * This is a test struct for testing parquet-thrift compatibility.
 */
struct ParquetThriftCompat {
    1: required bool boolColumn;
    2: required byte byteColumn;
    3: required i16 shortColumn;
    4: required i32 intColumn;
    5: required i64 longColumn;
    6: required double doubleColumn;
    7: required binary binaryColumn;
    8: required string stringColumn;
    9: required Suit enumColumn

    10: optional bool maybeBoolColumn;
    11: optional byte maybeByteColumn;
    12: optional i16 maybeShortColumn;
    13: optional i32 maybeIntColumn;
    14: optional i64 maybeLongColumn;
    15: optional double maybeDoubleColumn;
    16: optional binary maybeBinaryColumn;
    17: optional string maybeStringColumn;
    18: optional Suit maybeEnumColumn;

    19: required list<string> stringsColumn;
    20: required set<i32> intSetColumn;
    21: required map<i32, string> intToStringColumn;
    22: required map<i32, list<Nested>> complexColumn;
}
