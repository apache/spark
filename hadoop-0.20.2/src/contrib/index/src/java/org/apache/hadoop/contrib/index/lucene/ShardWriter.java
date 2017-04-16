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

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.contrib.index.mapred.IndexUpdateConfiguration;
import org.apache.hadoop.contrib.index.mapred.IntermediateForm;
import org.apache.hadoop.contrib.index.mapred.Shard;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;

/**
 * The initial version of an index is stored in the perm dir. Index files
 * created by newer versions are written to a temp dir on the local FS. After
 * successfully creating the new version in the temp dir, the shard writer
 * moves the new files to the perm dir and deletes the temp dir in close().
 */
public class ShardWriter {
  static final Log LOG = LogFactory.getLog(ShardWriter.class);

  private final FileSystem fs;
  private final FileSystem localFs;
  private final Path perm;
  private final Path temp;
  private final Directory dir;
  private final IndexWriter writer;
  private int maxNumSegments;
  private long numForms = 0;

  /**
   * Constructor
   * @param fs
   * @param shard
   * @param tempDir
   * @param iconf
   * @throws IOException
   */
  public ShardWriter(FileSystem fs, Shard shard, String tempDir,
      IndexUpdateConfiguration iconf) throws IOException {
    LOG.info("Construct a shard writer");

    this.fs = fs;
    localFs = FileSystem.getLocal(iconf.getConfiguration());
    perm = new Path(shard.getDirectory());
    temp = new Path(tempDir);

    long initGeneration = shard.getGeneration();
    if (!fs.exists(perm)) {
      assert (initGeneration < 0);
      fs.mkdirs(perm);
    } else {
      restoreGeneration(fs, perm, initGeneration);
    }
    dir =
        new MixedDirectory(fs, perm, localFs, fs.startLocalOutput(perm, temp),
            iconf.getConfiguration());

    // analyzer is null because we only use addIndexes, not addDocument
    writer =
        new IndexWriter(dir, false, null,
            initGeneration < 0 ? new KeepOnlyLastCommitDeletionPolicy()
                : new MixedDeletionPolicy());
    setParameters(iconf);
  }

  /**
   * Process an intermediate form by carrying out, on the Lucene instance of
   * the shard, the deletes and the inserts (a ram index) in the form. 
   * @param form  the intermediate form containing deletes and a ram index
   * @throws IOException
   */
  public void process(IntermediateForm form) throws IOException {
    // first delete
    Iterator<Term> iter = form.deleteTermIterator();
    while (iter.hasNext()) {
      writer.deleteDocuments(iter.next());
    }
    // then insert
    writer.addIndexesNoOptimize(new Directory[] { form.getDirectory() });
    numForms++;
  }

  /**
   * Close the shard writer. Optimize the Lucene instance of the shard before
   * closing if necessary, and copy the files created in the temp directory
   * to the permanent directory after closing.
   * @throws IOException
   */
  public void close() throws IOException {
    LOG.info("Closing the shard writer, processed " + numForms + " forms");
    try {
      try {
        if (maxNumSegments > 0) {
          writer.optimize(maxNumSegments);
          LOG.info("Optimized the shard into at most " + maxNumSegments
              + " segments");
        }
      } finally {
        writer.close();
        LOG.info("Closed Lucene index writer");
      }

      moveFromTempToPerm();
      LOG.info("Moved new index files to " + perm);

    } finally {
      dir.close();
      LOG.info("Closed the shard writer");
    }
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  public String toString() {
    return this.getClass().getName() + "@" + perm + "&" + temp;
  }

  private void setParameters(IndexUpdateConfiguration iconf) {
    int maxFieldLength = iconf.getIndexMaxFieldLength();
    if (maxFieldLength > 0) {
      writer.setMaxFieldLength(maxFieldLength);
    }
    writer.setUseCompoundFile(iconf.getIndexUseCompoundFile());
    maxNumSegments = iconf.getIndexMaxNumSegments();

    if (maxFieldLength > 0) {
      LOG.info("sea.max.field.length = " + writer.getMaxFieldLength());
    }
    LOG.info("sea.use.compound.file = " + writer.getUseCompoundFile());
    LOG.info("sea.max.num.segments = " + maxNumSegments);
  }

  // in case a previous reduce task fails, restore the generation to
  // the original starting point by deleting the segments.gen file
  // and the segments_N files whose generations are greater than the
  // starting generation; rest of the unwanted files will be deleted
  // once the unwanted segments_N files are deleted
  private void restoreGeneration(FileSystem fs, Path perm, long startGen)
      throws IOException {

    FileStatus[] fileStatus = fs.listStatus(perm, new PathFilter() {
      public boolean accept(Path path) {
        return LuceneUtil.isSegmentsFile(path.getName());
      }
    });

    // remove the segments_N files whose generation are greater than
    // the starting generation
    for (int i = 0; i < fileStatus.length; i++) {
      Path path = fileStatus[i].getPath();
      if (startGen < LuceneUtil.generationFromSegmentsFileName(path.getName())) {
        fs.delete(path);
      }
    }

    // always remove segments.gen in case last failed try removed segments_N
    // but not segments.gen, and segments.gen will be overwritten anyway.
    Path segmentsGenFile = new Path(LuceneUtil.IndexFileNames.SEGMENTS_GEN);
    if (fs.exists(segmentsGenFile)) {
      fs.delete(segmentsGenFile);
    }
  }

  // move the files created in the temp dir into the perm dir
  // and then delete the temp dir from the local FS
  private void moveFromTempToPerm() throws IOException {
    try {
      FileStatus[] fileStatus =
          localFs.listStatus(temp, LuceneIndexFileNameFilter.getFilter());
      Path segmentsPath = null;
      Path segmentsGenPath = null;

      // move the files created in temp dir except segments_N and segments.gen
      for (int i = 0; i < fileStatus.length; i++) {
        Path path = fileStatus[i].getPath();
        String name = path.getName();

        if (LuceneUtil.isSegmentsGenFile(name)) {
          assert (segmentsGenPath == null);
          segmentsGenPath = path;
        } else if (LuceneUtil.isSegmentsFile(name)) {
          assert (segmentsPath == null);
          segmentsPath = path;
        } else {
          fs.completeLocalOutput(new Path(perm, name), path);
        }
      }

      // move the segments_N file
      if (segmentsPath != null) {
        fs.completeLocalOutput(new Path(perm, segmentsPath.getName()),
            segmentsPath);
      }

      // move the segments.gen file
      if (segmentsGenPath != null) {
        fs.completeLocalOutput(new Path(perm, segmentsGenPath.getName()),
            segmentsGenPath);
      }
    } finally {
      // finally delete the temp dir (files should have been deleted)
      localFs.delete(temp);
    }
  }

}
