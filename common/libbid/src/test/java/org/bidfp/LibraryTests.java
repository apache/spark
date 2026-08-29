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
package org.bidfp;

import java.io.IOException;

import org.junit.jupiter.api.Test;

/** Runs the dependency-free {@code main} test classes under JUnit 5. */
class LibraryTests {
  @Test
  void bid64() {
    Bid64Test.main(new String[0]);
  }

  @Test
  void bid64IntelVectors() {
    Bid64IntelVectorTest.main(new String[0]);
  }

  @Test
  void bid64Compare() {
    Bid64CompareTest.main(new String[0]);
  }

  @Test
  void bid64Conversion() {
    Bid64ConversionTest.main(new String[0]);
  }

  @Test
  void bid64Add() throws IOException {
    Bid64AddTest.main(new String[0]);
  }

  @Test
  void bid64Multiply() throws IOException {
    Bid64MultiplyTest.main(new String[0]);
  }

  @Test
  void bid64Divide() throws IOException {
    Bid64DivideTest.main(new String[0]);
  }

  @Test
  void bidArithmeticVectors() throws IOException {
    BidArithmeticVectorTest.main(new String[0]);
  }

  @Test
  void bid64RawKernel() {
    Bid64RawKernelTest.main(new String[0]);
  }

  @Test
  void bid128() {
    Bid128Test.main(new String[0]);
  }

  @Test
  void bid128Add() throws IOException {
    Bid128AddTest.main(new String[0]);
  }

  @Test
  void bid128Multiply() throws IOException {
    Bid128MultiplyTest.main(new String[0]);
  }

  @Test
  void bid128Divide() throws IOException {
    Bid128DivideTest.main(new String[0]);
  }

  @Test
  void uint128() {
    UInt128Test.main(new String[0]);
  }

  @Test
  void bidRawApi() throws IOException {
    BidRawApiTest.main(new String[0]);
  }

  @Test
  void bidBinary128Convert() throws IOException {
    BidBinary128VectorTest.main(new String[0]);
  }

  @Test
  void bidTranscendentals() throws Exception {
    BidTranscendentalVectorTest.main(new String[0]);
  }

  @Test
  void bidComparisons() throws IOException {
    BidComparisonVectorTest.main(new String[0]);
  }

  @Test
  void bidRounding() throws IOException {
    BidRoundingVectorTest.main(new String[0]);
  }

  @Test
  void bidScale() throws IOException {
    BidScaleVectorTest.main(new String[0]);
  }

  @Test
  void bidNextMinMax() throws IOException {
    BidNextMinMaxVectorTest.main(new String[0]);
  }

  @Test
  void bidFmaRem() throws IOException {
    BidFmaRemVectorTest.main(new String[0]);
  }

  @Test
  void bidDpd() throws IOException {
    BidDpdVectorTest.main(new String[0]);
  }

  @Test
  void bidIntegerConversions() throws IOException {
    BidIntegerVectorTest.main(new String[0]);
  }

  @Test
  void bidMisc() throws IOException {
    BidMiscVectorTest.main(new String[0]);
  }

  @Test
  void bidUtilities() throws IOException {
    BidUtilityVectorTest.main(new String[0]);
  }

  @Test
  void bidObjectApi() {
    BidObjectApiTest.main(new String[0]);
  }
}
