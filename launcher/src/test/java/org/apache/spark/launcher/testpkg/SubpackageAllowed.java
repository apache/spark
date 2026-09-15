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

package org.apache.spark.launcher.testpkg;

import java.io.Serializable;

/**
 * Test-only fixture for {@code FilteredObjectInputStreamSuite}. Its FQCN starts with the
 * "org.apache.spark.launcher." prefix from ALLOWED_PACKAGES in {@code
 * FilteredObjectInputStream.resolveClass} without being in the {@code org.apache.spark.launcher}
 * package itself, documenting that the prefix match over-admits subpackages here just as it does
 * for "java.lang." - unlike the java.lang. case, the JVM does not block user-defined classes from
 * this package name, so this fixture can be exercised with a normal round trip.
 */
public class SubpackageAllowed implements Serializable {
}
