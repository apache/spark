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
import java.nio.file.Files;

import org.apache.spark.network.util.JavaUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class LocalDirValidatorSuite {

  /**
   * With local-directory roots configured, validate() must reject any localDir that does not
   * canonicalize to a path under one of those roots.
   */
  private static void assertRejectsLocalDir(String[] localDirs) throws Exception {
    File root = Files.createTempDirectory("ess-local-root").toFile();
    try {
      LocalDirValidator validator = new LocalDirValidator(new String[] { root.getPath() }, false);
      assertThrows(IllegalArgumentException.class, () -> validator.validate(localDirs, "app0"));
    } finally {
      JavaUtils.deleteRecursively(root);
    }
  }

  @Test
  public void rejectsRootLocalDir() throws Exception {
    assertRejectsLocalDir(new String[] { File.separator });
  }

  @Test
  public void rejectsParentDirSegment() throws Exception {
    assertRejectsLocalDir(new String[] {
      "/tmp" + File.separator + "x" + File.separator + ".." + File.separator + ".." +
        File.separator + "etc" });
  }

  @Test
  public void rejectsNullEntry() throws Exception {
    assertRejectsLocalDir(new String[] { null });
  }

  @Test
  public void rejectsDirOutsideConfiguredRoots() throws Exception {
    assertRejectsLocalDir(new String[] { "/etc/yarn" });
  }

  @Test
  public void rejectsConfiguredRootItself() throws Exception {
    File root = Files.createTempDirectory("ess-local-root").toFile();
    try {
      LocalDirValidator validator = new LocalDirValidator(new String[] { root.getPath() }, false);
      assertThrows(IllegalArgumentException.class,
        () -> validator.validate(new String[] { root.getPath() }, "app0"),
        "validate must reject a localDir that is a configured root itself");
    } finally {
      JavaUtils.deleteRecursively(root);
    }
  }

  @Test
  public void rejectsConfiguredRootBelowAnotherRoot() throws Exception {
    File outerRoot = Files.createTempDirectory("ess-local-root").toFile();
    try {
      File betweenRoots = new File(outerRoot, "a");
      File innerRoot = new File(betweenRoots, "inner");
      File ownDir = new File(innerRoot, "spark-x/executor-x/blockmgr-x");
      assertTrue(ownDir.mkdirs(), "precondition: directory created");
      LocalDirValidator validator = new LocalDirValidator(
        new String[] { outerRoot.getPath(), innerRoot.getPath() }, false);
      assertThrows(IllegalArgumentException.class,
        () -> validator.validate(new String[] { innerRoot.getPath() }, "app0"),
        "validate must reject a configured root even when it is below another root");
      assertThrows(IllegalArgumentException.class,
        () -> validator.validate(new String[] { betweenRoots.getPath() }, "app0"),
        "validate must reject a localDir that contains a configured root");
      // Must not throw.
      validator.validate(new String[] { ownDir.getPath() }, "app0");
    } finally {
      JavaUtils.deleteRecursively(outerRoot);
    }
  }

  @Test
  public void rejectsAnotherApplicationsDir() throws Exception {
    File root = Files.createTempDirectory("ess-nm-local").toFile();
    try {
      File otherAppDir = new File(root, "usercache/u/appcache/app1/blockmgr-x");
      assertTrue(otherAppDir.mkdirs(), "precondition: directory created");
      LocalDirValidator validator = new LocalDirValidator(new String[] { root.getPath() }, true);
      assertThrows(IllegalArgumentException.class,
        () -> validator.validate(new String[] { otherAppDir.getPath() }, "app0"),
        "validate must reject a localDir scoped to a different application");
    } finally {
      JavaUtils.deleteRecursively(root);
    }
  }

  @Test
  public void acceptsContainedAppScopedDir() throws Exception {
    File root = Files.createTempDirectory("ess-nm-local").toFile();
    try {
      File ownDir = new File(root, "usercache/u/appcache/app0/blockmgr-x");
      assertTrue(ownDir.mkdirs(), "precondition: directory created");
      LocalDirValidator validator = new LocalDirValidator(new String[] { root.getPath() }, true);
      // Must not throw.
      validator.validate(new String[] { ownDir.getPath() }, "app0");
    } finally {
      JavaUtils.deleteRecursively(root);
    }
  }
}
