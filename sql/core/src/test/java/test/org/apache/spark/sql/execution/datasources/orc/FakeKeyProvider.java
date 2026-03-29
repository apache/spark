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

package test.org.apache.spark.sql.execution.datasources.orc;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;

/**
 * A Hadoop KeyProvider that lets us test the interaction
 * with the Hadoop code.
 *
 * https://github.com/apache/orc/blob/rel/release-1.6.7/java/tools/src/test/org/apache/orc/impl/FakeKeyProvider.java
 *
 * This file intentionally keeps the original file except
 * (1) package name, (2) import order, (3) a few indentation
 */
public class FakeKeyProvider extends KeyProvider {
  // map from key name to metadata
  private final Map<String, TestMetadata> keyMetdata = new HashMap<>();
  // map from key version name to material
  private final Map<String, KeyVersion> keyVersions = new HashMap<>();

  public FakeKeyProvider(Configuration conf) {
    super(conf);
  }

  @Override
  public KeyVersion getKeyVersion(String name) {
    return keyVersions.get(name);
  }

  @Override
  public List<String> getKeys() {
    return new ArrayList<>(keyMetdata.keySet());
  }

  @Override
  public List<KeyVersion> getKeyVersions(String name) {
    List<KeyVersion> result = new ArrayList<>();
    Metadata meta = getMetadata(name);
    for(int v=0; v < meta.getVersions(); ++v) {
      String versionName = buildVersionName(name, v);
      KeyVersion material = keyVersions.get(versionName);
      if (material != null) {
        result.add(material);
      }
    }
    return result;
  }

  @Override
  public Metadata getMetadata(String name)  {
    return keyMetdata.get(name);
  }

  @Override
  public KeyVersion createKey(String name, byte[] bytes, Options options) {
    String versionName = buildVersionName(name, 0);
    keyMetdata.put(name, new TestMetadata(options.getCipher(),
        options.getBitLength(), 1));
    KeyVersion result = new KMSClientProvider.KMSKeyVersion(name, versionName, bytes);
    keyVersions.put(versionName, result);
    return result;
  }

  @Override
  public void deleteKey(String name) {
    throw new UnsupportedOperationException("Can't delete keys");
  }

  @Override
  public KeyVersion rollNewVersion(String name, byte[] bytes) {
    TestMetadata key = keyMetdata.get(name);
    String versionName = buildVersionName(name, key.addVersion());
    KeyVersion result = new KMSClientProvider.KMSKeyVersion(name, versionName,
        bytes);
    keyVersions.put(versionName, result);
    return result;
  }

  @Override
  public void flush() {
    // Nothing
  }

  static class TestMetadata extends KeyProvider.Metadata {

    TestMetadata(String cipher, int bitLength, int versions) {
      super(cipher, bitLength, null, null, null, versions);
    }

    public int addVersion() {
      return super.addVersion();
    }
  }

  public static class Factory extends KeyProviderFactory {

    @Override
    public KeyProvider createProvider(URI uri, Configuration conf) throws IOException {
      if ("test".equals(uri.getScheme())) {
        KeyProvider provider = new FakeKeyProvider(conf);
        // populate a couple keys into the provider
        byte[] piiKey = new byte[]{0,1,2,3,4,5,6,7,8,9,0xa,0xb,0xc,0xd,0xe,0xf};
        org.apache.hadoop.crypto.key.KeyProvider.Options aes128 = new KeyProvider.Options(conf);
        provider.createKey("pii", piiKey, aes128);
        byte[] piiKey2 = new byte[]{0x10,0x11,0x12,0x13,0x14,0x15,0x16,0x17,
            0x18,0x19,0x1a,0x1b,0x1c,0x1d,0x1e,0x1f};
        provider.rollNewVersion("pii", piiKey2);
        byte[] secretKey = new byte[]{0x20,0x21,0x22,0x23,0x24,0x25,0x26,0x27,
            0x28,0x29,0x2a,0x2b,0x2c,0x2d,0x2e,0x2f};
        provider.createKey("secret", secretKey, aes128);
        return KeyProviderCryptoExtension.createKeyProviderCryptoExtension(provider);
      }
      return null;
    }
  }
}
