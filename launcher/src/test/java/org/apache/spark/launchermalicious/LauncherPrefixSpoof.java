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

package org.apache.spark.launchermalicious;

import java.io.Serializable;

/**
 * Test-only fixture for {@code FilteredObjectInputStreamSuite}. Its FQCN shares the
 * "org.apache.spark.launcher" text with the SPARK-20922 allow-list without being in
 * that package, pinning down the trailing-dot boundary of ALLOWED_PACKAGES in
 * {@code FilteredObjectInputStream.resolveClass}.
 */
public class LauncherPrefixSpoof implements Serializable {
}
