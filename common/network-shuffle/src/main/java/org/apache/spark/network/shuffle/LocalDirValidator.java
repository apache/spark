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

package org.apache.spark.network.shuffle;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;

/**
 * Validates the local directories an executor reports at registration, before the external shuffle
 * service turns them into filesystem paths. {@link ExternalBlockHandler} owns one of these (built
 * from the host's configured local-directory roots) and applies it at the RPC boundary. So both the
 * block resolver and the merged-shuffle manager get validated paths at registration.
 *
 * Each entry must canonicalize to a path below one of the configured roots (YARN passes
 * {@code yarn.nodemanager.local-dirs}, standalone the configured local directories), and must
 * neither be a root nor contain one. When app scoping is required, it must also be within the
 * registering application's own directory (the {@code appId} must appear as a path segment). When
 * no roots are configured there is nothing to contain the entries against and they are accepted.
 * An empty array is always allowed.
 *
 * The block resolver also applies it to executors reloaded from its state store.
 */
class LocalDirValidator {
  private static final SparkLogger logger = SparkLoggerFactory.getLogger(LocalDirValidator.class);

  /** A validator without configured roots, which accepts every local dir. */
  static final LocalDirValidator ACCEPT_ALL = new LocalDirValidator(null, false);

  private final List<Path> allowedLocalDirRoots;
  private final boolean requireAppScopedLocalDirs;

  LocalDirValidator(String[] allowedLocalDirs, boolean requireAppScopedLocalDirs) {
    this.allowedLocalDirRoots = canonicalizeRoots(allowedLocalDirs);
    this.requireAppScopedLocalDirs = requireAppScopedLocalDirs;
  }

  void validate(String[] localDirs, String appId) {
    if (allowedLocalDirRoots.isEmpty()) {
      return;
    }
    if (localDirs == null) {
      throw new IllegalArgumentException("localDirs must not be null");
    }
    for (String localDir : localDirs) {
      if (localDir == null || localDir.isEmpty()) {
        throw new IllegalArgumentException("localDirs entries must be non-null and non-empty");
      }
      validateContained(localDir, appId);
    }
  }

  private void validateContained(String localDir, String appId) {
    Path canonical;
    try {
      canonical = new File(localDir).getCanonicalFile().toPath();
    } catch (IOException e) {
      throw new IllegalArgumentException(
        "localDirs entry could not be canonicalized: " + localDir, e);
    }
    // A root holds the directories of every application, so an entry must neither be a root nor
    // contain one.
    if (allowedLocalDirRoots.stream().anyMatch(root -> root.startsWith(canonical))) {
      throw new IllegalArgumentException(
        "localDirs entry is or contains one of the service's local directories: " + localDir);
    }
    if (allowedLocalDirRoots.stream().noneMatch(canonical::startsWith)) {
      throw new IllegalArgumentException(
        "localDirs entry is not under any of the service's local directories: " + localDir);
    }
    if (requireAppScopedLocalDirs &&
        (appId == null || appId.isEmpty() || !pathContainsSegment(canonical, appId))) {
      throw new IllegalArgumentException(
        "localDirs entry is not within application " + appId + "'s directory: " + localDir);
    }
  }

  private static boolean pathContainsSegment(Path path, String segment) {
    for (Path part : path) {
      if (part.toString().equals(segment)) {
        return true;
      }
    }
    return false;
  }

  private static List<Path> canonicalizeRoots(String[] allowedLocalDirs) {
    List<Path> roots = new ArrayList<>();
    if (allowedLocalDirs != null) {
      for (String root : allowedLocalDirs) {
        if (root == null || root.isEmpty()) {
          continue;
        }
        try {
          roots.add(new File(root).getCanonicalFile().toPath());
        } catch (IOException e) {
          logger.warn("Ignoring local dir root that could not be canonicalized: {}",
            MDC.of(LogKeys.PATH, root));
        }
      }
    }
    return roots;
  }
}
