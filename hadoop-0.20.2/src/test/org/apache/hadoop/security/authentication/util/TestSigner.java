/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.util;

import junit.framework.TestCase;

public class TestSigner extends TestCase {

  public void testNoSecret() throws Exception {
    try {
      new Signer(null);
      fail();
    }
    catch (IllegalArgumentException ex) {
    }
  }

  public void testNullAndEmptyString() throws Exception {
    Signer signer = new Signer("secret".getBytes());
    try {
      signer.sign(null);
      fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      fail();
    }
    try {
      signer.sign("");
      fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      fail();
    }
  }

  public void testSignature() throws Exception {
    Signer signer = new Signer("secret".getBytes());
    String s1 = signer.sign("ok");
    String s2 = signer.sign("ok");
    String s3 = signer.sign("wrong");
    assertEquals(s1, s2);
    assertNotSame(s1, s3);
  }

  public void testVerify() throws Exception {
    Signer signer = new Signer("secret".getBytes());
    String t = "test";
    String s = signer.sign(t);
    String e = signer.verifyAndExtract(s);
    assertEquals(t, e);
  }

  public void testInvalidSignedText() throws Exception {
    Signer signer = new Signer("secret".getBytes());
    try {
      signer.verifyAndExtract("test");
      fail();
    } catch (SignerException ex) {
      // Expected
    } catch (Throwable ex) {
      fail();
    }
  }

  public void testTampering() throws Exception {
    Signer signer = new Signer("secret".getBytes());
    String t = "test";
    String s = signer.sign(t);
    s += "x";
    try {
      signer.verifyAndExtract(s);
      fail();
    } catch (SignerException ex) {
      // Expected
    } catch (Throwable ex) {
      fail();
    }
  }

}
