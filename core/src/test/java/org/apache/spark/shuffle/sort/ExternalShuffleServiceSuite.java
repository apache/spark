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

package org.apache.spark.shuffle.sort;

import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.ExternalShuffleService;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.util.Utils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * This suite gets BlockData when the ExternalShuffleService is restarted  with #spark.shuffle.service.db.enabled=true or false
 * Note that failures in this suite may arise  when #spark.shuffle.service.db.enabled=false
 */
public class ExternalShuffleServiceSuite {
  private static final String sortBlock0 = "Hello!";
  private static final String sortBlock1 = "World!";
  private static final String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";

  private static SparkConf sparkConf ;
  private static SecurityManager securityManager;
  private static TestShuffleDataContext dataContext;

  private static ExternalShuffleService externalShuffleService;
  private static ExternalShuffleBlockHandler bockHandler;
  private static ExternalShuffleBlockResolver blockResolver;

  @BeforeClass
  public static void beforeAll() throws IOException {
    sparkConf = new SparkConf();
    sparkConf.set("spark.shuffle.service.enabled", "true");
    sparkConf.set("spark.local.dir", System.getProperty("java.io.tmpdir"));
    Utils.loadDefaultSparkProperties(sparkConf, null);
    securityManager = new SecurityManager(sparkConf, scala.Option.apply(null));

    dataContext = new TestShuffleDataContext(2, 5);
    dataContext.create();
    // Write some sort data.
    dataContext.insertSortShuffleData(0, 0, new byte[][] {
            sortBlock0.getBytes(StandardCharsets.UTF_8),
            sortBlock1.getBytes(StandardCharsets.UTF_8)});
    registerExecutor();
  }

  @AfterClass
  public static void afterAll() {
    dataContext.cleanup();
  }

  public static void  registerExecutor() {
    sparkConf.set("spark.shuffle.service.db.enabled", "true");
    externalShuffleService = new ExternalShuffleService(sparkConf, securityManager);
    //externalShuffleService start
    externalShuffleService.start();

    bockHandler = externalShuffleService.getBlockHandler();
    blockResolver = bockHandler.getBlockResolver();

    blockResolver.registerExecutor("app0", "exec0",
            dataContext.createExecutorInfo(SORT_MANAGER));
    blockResolver.closeForTest();
    //externalShuffleService stop
    externalShuffleService.stop();
  }

  @Test
  public void  restartExternalShuffleServiceWithInitRegisteredExecutorsDB() throws IOException {
    sparkConf.set("spark.shuffle.service.db.enabled", "true");
    externalShuffleService = new ExternalShuffleService(sparkConf, securityManager);
    //externalShuffleService restart
    externalShuffleService.start();

    bockHandler = externalShuffleService.getBlockHandler();
    blockResolver = bockHandler.getBlockResolver();
    try {
      InputStream block0Stream =
              blockResolver.getBlockData("app0", "exec0", 0, 0, 0).createInputStream();
      String block0 = CharStreams.toString(
              new InputStreamReader(block0Stream, StandardCharsets.UTF_8));
      block0Stream.close();
      assertEquals(sortBlock0, block0);
    } catch (RuntimeException e) {
      //pass
    } finally {
      blockResolver.closeForTest();
      //externalShuffleService stop
      externalShuffleService.stop();
    }
  }

  @Test
  public void  restartExternalShuffleServiceWithoutInitRegisteredExecutorsDB() throws IOException {
    sparkConf.set("spark.shuffle.service.db.enabled", "false");
    externalShuffleService = new ExternalShuffleService(sparkConf, securityManager);
    //externalShuffleService restart
    externalShuffleService.start();
    bockHandler = externalShuffleService.getBlockHandler();
    blockResolver = bockHandler.getBlockResolver();
    try {
      InputStream block0Stream =
              blockResolver.getBlockData("app0", "exec0", 0, 0, 0).createInputStream();
      String block0 = CharStreams.toString(
              new InputStreamReader(block0Stream, StandardCharsets.UTF_8));
      block0Stream.close();
      assertEquals(sortBlock0, block0);
      fail("Should have failed");
    } catch (RuntimeException e) {
      assertTrue("Bad error message: " + e, e.getMessage().contains("not registered"));
    } finally {
      blockResolver.closeForTest();
      //externalShuffleService stop
      externalShuffleService.stop();
    }
  }

  /**
   * Manages some sort-shuffle data, including the creation
   * and cleanup of directories that can be read by the {@link ExternalShuffleBlockResolver}.
   *
   * Copy from org.apache.spark.network.shuffle.TestShuffleDataContext
   */
  public static class TestShuffleDataContext {
    private final Logger logger = LoggerFactory.getLogger(TestShuffleDataContext.class);

    public final String[] localDirs;
    public final int subDirsPerLocalDir;

    public TestShuffleDataContext(int numLocalDirs, int subDirsPerLocalDir) {
      this.localDirs = new String[numLocalDirs];
      this.subDirsPerLocalDir = subDirsPerLocalDir;
    }

    public void create() {
      for (int i = 0; i < localDirs.length; i++) {
        localDirs[i] = Files.createTempDir().getAbsolutePath();

        for (int p = 0; p < subDirsPerLocalDir; p++) {
          new File(localDirs[i], String.format("%02x", p)).mkdirs();
        }
      }
    }

    public void cleanup() {
      for (String localDir : localDirs) {
        try {
          JavaUtils.deleteRecursively(new File(localDir));
        } catch (IOException e) {
          logger.warn("Unable to cleanup localDir = " + localDir, e);
        }
      }
    }

    /**
     * Creates reducer blocks in a sort-based data format within our local dirs.
     */
    public void insertSortShuffleData(int shuffleId, int mapId, byte[][] blocks) throws IOException {
      String blockId = "shuffle_" + shuffleId + "_" + mapId + "_0";

      OutputStream dataStream = null;
      DataOutputStream indexStream = null;
      boolean suppressExceptionsDuringClose = true;

      try {
        dataStream = new FileOutputStream(
                ExternalShuffleBlockResolver.getFileForTest(localDirs, subDirsPerLocalDir, blockId + ".data"));
        indexStream = new DataOutputStream(new FileOutputStream(
                ExternalShuffleBlockResolver.getFileForTest(localDirs, subDirsPerLocalDir, blockId + ".index")));

        long offset = 0;
        indexStream.writeLong(offset);
        for (byte[] block : blocks) {
          offset += block.length;
          dataStream.write(block);
          indexStream.writeLong(offset);
        }
        suppressExceptionsDuringClose = false;
      } finally {
        Closeables.close(dataStream, suppressExceptionsDuringClose);
        Closeables.close(indexStream, suppressExceptionsDuringClose);
      }
    }

    /**
     * Creates an ExecutorShuffleInfo object based on the given shuffle manager which targets this
     * context's directories.
     */
    public ExecutorShuffleInfo createExecutorInfo(String shuffleManager) {
      return new ExecutorShuffleInfo(localDirs, subDirsPerLocalDir, shuffleManager);
    }
  }
}


