/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.bidfp.binary128.tables.CbrtX;
import org.bidfp.binary128.tables.ConsX;
import org.bidfp.binary128.tables.ErfX;
import org.bidfp.binary128.tables.ExpX;
import org.bidfp.binary128.tables.FourOverPi;
import org.bidfp.binary128.tables.InvHyperX;
import org.bidfp.binary128.tables.InvTrigX;
import org.bidfp.binary128.tables.LgammaX;
import org.bidfp.binary128.tables.LogX;
import org.bidfp.binary128.tables.PowX;
import org.bidfp.binary128.tables.TableData;
import org.bidfp.binary128.tables.TrigX;
import org.junit.jupiter.api.Test;

/** Verifies generated tables and retained Intel resources byte-for-byte. */
public final class GeneratedDataChecksumTest {
  @Test
  void generatedTableChecksums() {
    assertTable(
        ConsX.TABLE,
        "b51093963bddb9d80d6ae4fa84d23ead6d0f052c575ba214dfb032da4722bddd");
    assertTable(
        ExpX.TABLE,
        "162b9a8ba716e85fd1db00c2ad45c50b209f7596053f13a4a7171b52f6f4735b");
    assertTable(
        LogX.TABLE,
        "ca7b9e324f6d3ffd54cce551cc330e13731bd9c339d364c03cbf8969c78ee8e9");
    assertTable(
        PowX.TABLE,
        "c79bbb49b289d309536bc4c72f51891cc7f84378d817501e15238b31de8c3074");
    assertTable(
        CbrtX.TABLE,
        "6b652b78cdd11efe8209941a14fffce44d5805b7c9e82f8ad2711e433d9c3fa8");
    assertTable(
        TrigX.TABLE,
        "461c4295e521616113d36d1f9e5992afe695e118cabda39a20e9cbf0ddea23b1");
    assertTable(
        InvTrigX.TABLE,
        "51651509bd5c3fda2179ba65b19895808a8e66041f7303c9fb905a0e7e4ea092");
    assertTable(
        InvHyperX.TABLE,
        "a9ecf60dc85749d76d6213e242c95136036bb8ffe5ef9abda07c8d6057313599");
    assertTable(
        ErfX.TABLE,
        "8ee8e332b316d9bc3a46576606b72476da1c787e77501daee8a9a9ca0b4fb657");
    assertTable(
        LgammaX.TABLE,
        "d823484cccf35200a87d55a94fd5557b552f2612fa38b7102d657d0ff0f2303b");
    assertTable(
        FourOverPi.TABLE,
        "16b24b3d32f0713338d7f62c235fbc7c1963686e6f1b01c92d12249b7441a8bf");
  }

  @Test
  void retainedResourceChecksums() throws IOException {
    assertResource(
        "/org/bidfp/bid128_sin_moduli.bin",
        "84a7a565d0652390c1e1dd90d2b4b612ba8a42870b0263a886621a15d61aca93");
    assertResource(
        "/org/bidfp/binary128/intel-f128-oracle.txt",
        "65b8af10af127776f8df911bd2099b450143684a424e91b117ba09ddd7744e99");
    assertResource(
        "/org/bidfp/readtest.in",
        "bb7f2ccae62f5d6d1b6261b891194668506b3291b065feda8531cb90d113abd2");
  }

  private static void assertTable(TableData table, String expected) {
    MessageDigest digest = sha256();
    ByteBuffer bytes = ByteBuffer.allocate(Long.BYTES);
    for (int index = 0; index < table.length(); index++) {
      bytes.clear();
      bytes.putLong(table.get(index));
      digest.update(bytes.array());
    }
    assertEquals(expected, hex(digest.digest()));
  }

  private static void assertResource(String name, String expected) throws IOException {
    MessageDigest digest = sha256();
    try (InputStream input = GeneratedDataChecksumTest.class.getResourceAsStream(name)) {
      if (input == null) {
        throw new IllegalStateException("missing test resource " + name);
      }
      byte[] buffer = new byte[8192];
      int count;
      while ((count = input.read(buffer)) >= 0) {
        digest.update(buffer, 0, count);
      }
    }
    assertEquals(expected, hex(digest.digest()));
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException(error);
    }
  }

  private static String hex(byte[] bytes) {
    StringBuilder result = new StringBuilder(bytes.length * 2);
    for (byte value : bytes) {
      result.append(String.format("%02x", value & 0xff));
    }
    return result.toString();
  }
}
