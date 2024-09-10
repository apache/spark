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

package org.apache.spark.sql.catalyst.util;

/**
 * 'SpecialCodePointConstants' is introduced in order to keep the codepoints used in
 * 'CollationAwareUTF8String' in one place.
 */
public class SpecialCodePointConstants {

    public static final int COMBINING_DOT = 0x0307;
    public static final int ASCII_SMALL_I = 0x0069;
    public static final int ASCII_SPACE = 0x0020;
    public static final int GREEK_CAPITAL_SIGMA = 0x03A3;
    public static final int GREEK_SMALL_SIGMA = 0x03C3;
    public static final int GREEK_FINAL_SIGMA = 0x03C2;
    public static final int CAPITAL_I_WITH_DOT_ABOVE = 0x0130;
}
