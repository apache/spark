/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.filecache;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestURIFragments {

  /**
   * Tests {@link TrackerDistributedCacheManager#checkURIs(URI[], URI[]).
   */
  @Test
  public void testURIs() throws URISyntaxException {
    assertTrue(DistributedCache.checkURIs(null, null));

    // uris with no fragments
    assertFalse(DistributedCache.checkURIs(new URI[] { new URI(
        "file://foo/bar/myCacheFile.txt") }, null));
    assertFalse(DistributedCache.checkURIs(null,
        new URI[] { new URI("file://foo/bar/myCacheArchive.txt") }));
    assertFalse(DistributedCache.checkURIs(new URI[] {
        new URI("file://foo/bar/myCacheFile1.txt#file"),
        new URI("file://foo/bar/myCacheFile2.txt") }, null));
    assertFalse(DistributedCache.checkURIs(null, new URI[] {
        new URI("file://foo/bar/myCacheArchive1.txt"),
        new URI("file://foo/bar/myCacheArchive2.txt#archive") }));
    assertFalse(DistributedCache.checkURIs(new URI[] { new URI(
        "file://foo/bar/myCacheFile.txt") }, new URI[] { new URI(
        "file://foo/bar/myCacheArchive.txt") }));

    // conflicts in fragment names
    assertFalse(DistributedCache.checkURIs(new URI[] {
        new URI("file://foo/bar/myCacheFile1.txt#file"),
        new URI("file://foo/bar/myCacheFile2.txt#file") }, null));
    assertFalse(DistributedCache.checkURIs(null, new URI[] {
        new URI("file://foo/bar/myCacheArchive1.txt#archive"),
        new URI("file://foo/bar/myCacheArchive2.txt#archive") }));
    assertFalse(DistributedCache.checkURIs(new URI[] { new URI(
        "file://foo/bar/myCacheFile.txt#cache") }, new URI[] { new URI(
        "file://foo/bar/myCacheArchive.txt#cache") }));
    assertFalse(DistributedCache.checkURIs(new URI[] {
        new URI("file://foo/bar/myCacheFile1.txt#file1"),
        new URI("file://foo/bar/myCacheFile2.txt#file2") }, new URI[] {
        new URI("file://foo/bar/myCacheArchive1.txt#archive"),
        new URI("file://foo/bar/myCacheArchive2.txt#archive") }));
    assertFalse(DistributedCache.checkURIs(new URI[] {
        new URI("file://foo/bar/myCacheFile1.txt#file"),
        new URI("file://foo/bar/myCacheFile2.txt#file") }, new URI[] {
        new URI("file://foo/bar/myCacheArchive1.txt#archive1"),
        new URI("file://foo/bar/myCacheArchive2.txt#archive2") }));
    assertFalse(DistributedCache.checkURIs(new URI[] {
        new URI("file://foo/bar/myCacheFile1.txt#file1"),
        new URI("file://foo/bar/myCacheFile2.txt#cache") }, new URI[] {
        new URI("file://foo/bar/myCacheArchive1.txt#cache"),
        new URI("file://foo/bar/myCacheArchive2.txt#archive2") }));

    // test ignore case
    assertFalse(DistributedCache.checkURIs(new URI[] {
        new URI("file://foo/bar/myCacheFile1.txt#file"),
        new URI("file://foo/bar/myCacheFile2.txt#FILE") }, null));
    assertFalse(DistributedCache.checkURIs(null, new URI[] {
        new URI("file://foo/bar/myCacheArchive1.txt#archive"),
        new URI("file://foo/bar/myCacheArchive2.txt#ARCHIVE") }));
    assertFalse(DistributedCache.checkURIs(new URI[] { new URI(
        "file://foo/bar/myCacheFile.txt#cache") }, new URI[] { new URI(
        "file://foo/bar/myCacheArchive.txt#CACHE") }));
    assertFalse(DistributedCache.checkURIs(new URI[] {
        new URI("file://foo/bar/myCacheFile1.txt#file1"),
        new URI("file://foo/bar/myCacheFile2.txt#file2") }, new URI[] {
        new URI("file://foo/bar/myCacheArchive1.txt#ARCHIVE"),
        new URI("file://foo/bar/myCacheArchive2.txt#archive") }));
    assertFalse(DistributedCache.checkURIs(new URI[] {
        new URI("file://foo/bar/myCacheFile1.txt#FILE"),
        new URI("file://foo/bar/myCacheFile2.txt#file") }, new URI[] {
        new URI("file://foo/bar/myCacheArchive1.txt#archive1"),
        new URI("file://foo/bar/myCacheArchive2.txt#archive2") }));
    assertFalse(DistributedCache.checkURIs(new URI[] {
        new URI("file://foo/bar/myCacheFile1.txt#file1"),
        new URI("file://foo/bar/myCacheFile2.txt#CACHE") }, new URI[] {
        new URI("file://foo/bar/myCacheArchive1.txt#cache"),
        new URI("file://foo/bar/myCacheArchive2.txt#archive2") }));

    // allowed uri combinations
    assertTrue(DistributedCache.checkURIs(new URI[] {
        new URI("file://foo/bar/myCacheFile1.txt#file1"),
        new URI("file://foo/bar/myCacheFile2.txt#file2") }, null));
    assertTrue(DistributedCache.checkURIs(null, new URI[] {
        new URI("file://foo/bar/myCacheArchive1.txt#archive1"),
        new URI("file://foo/bar/myCacheArchive2.txt#archive2") }));
    assertTrue(DistributedCache.checkURIs(new URI[] {
        new URI("file://foo/bar/myCacheFile1.txt#file1"),
        new URI("file://foo/bar/myCacheFile2.txt#file2") }, new URI[] {
        new URI("file://foo/bar/myCacheArchive1.txt#archive1"),
        new URI("file://foo/bar/myCacheArchive2.txt#archive2") }));
  }
}
