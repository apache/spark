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

package org.apache.hadoop.contrib.index.lucene;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.lucene.index.IndexFileNameFilter;

/**
 * A wrapper class to convert an IndexFileNameFilter which implements
 * java.io.FilenameFilter to an org.apache.hadoop.fs.PathFilter.
 */
class LuceneIndexFileNameFilter implements PathFilter {

  private static final LuceneIndexFileNameFilter singleton =
      new LuceneIndexFileNameFilter();

  /**
   * Get a static instance.
   * @return the static instance
   */
  public static LuceneIndexFileNameFilter getFilter() {
    return singleton;
  }

  private final IndexFileNameFilter luceneFilter;

  private LuceneIndexFileNameFilter() {
    luceneFilter = IndexFileNameFilter.getFilter();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.PathFilter#accept(org.apache.hadoop.fs.Path)
   */
  public boolean accept(Path path) {
    return luceneFilter.accept(null, path.getName());
  }

}
