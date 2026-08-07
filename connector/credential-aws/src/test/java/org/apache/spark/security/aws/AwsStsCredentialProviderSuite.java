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

package org.apache.spark.security.aws;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.security.CredentialProvider;

/**
 * Tests that {@link AwsStsCredentialProvider} is discoverable via {@link ServiceLoader}.
 */
public class AwsStsCredentialProviderSuite {

  @Test
  void testServiceLoaderDiscovery() {
    ServiceLoader<CredentialProvider> loader = ServiceLoader.load(CredentialProvider.class);
    boolean found = false;
    for (CredentialProvider provider : loader) {
      if (provider instanceof AwsStsCredentialProvider) {
        found = true;
        break;
      }
    }
    assertTrue(found, "AwsStsCredentialProvider should be discoverable via ServiceLoader");
  }

  @Test
  void testSupportedSchemes() {
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();
    Set<String> schemes = provider.supportedSchemes();
    assertEquals(Set.of("s3a"), schemes);
  }

  @Test
  void testInitThrowsUnsupported() {
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();
    assertThrows(UnsupportedOperationException.class, () -> provider.init(Map.of()));
  }

  @Test
  void testResolveThrowsUnsupported() {
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();
    assertThrows(UnsupportedOperationException.class,
        () -> provider.resolve(null, URI.create("s3a://bucket/key")));
  }

  @Test
  void testSuggestedTtl() {
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();
    assertEquals(Duration.ofMinutes(15), provider.suggestedTtl());
  }
}
