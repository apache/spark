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

namespace java org.apache.spark.sql.parquet.test.thrift

enum Suit {
    SPADES,
    HEARTS,
    DIAMONDS,
    CLUBS
}

struct Nested {
    1: list<i32> nestedIntsColumn;
    2: string nestedStringColumn;
}

/**
 * This is a test struct for testing parquet-thrift compatibility.
 */
struct ParquetThriftCompat {
    1: bool boolColumn;
    2: byte byteColumn;
    3: i16 shortColumn;
    4: i32 intColumn;
    5: i64 longColumn;
    6: double doubleColumn;
    7: binary binaryColumn;
    8: string stringColumn;
    9: Suit enumColumn

    10: optional bool maybeBoolColumn;
    11: optional byte maybeByteColumn;
    12: optional i16 maybeShortColumn;
    13: optional i32 maybeIntColumn;
    14: optional i64 maybeLongColumn;
    15: optional double maybeDoubleColumn;
    16: optional binary maybeBinaryColumn;
    17: optional string maybeStringColumn;
    18: optional Suit maybeEnumColumn;

    19: list<string> stringsColumn;
    20: set<i32> intSetColumn;
    21: map<i32, string> intToStringColumn;
    22: map<i32, list<Nested>> complexColumn;
}
