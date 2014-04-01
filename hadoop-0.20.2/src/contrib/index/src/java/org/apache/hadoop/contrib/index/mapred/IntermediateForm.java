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

package org.apache.hadoop.contrib.index.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.contrib.index.lucene.RAMDirectoryUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/**
 * An intermediate form for one or more parsed Lucene documents and/or
 * delete terms. It actually uses Lucene file format as the format for
 * the intermediate form by using RAM dir files.
 * 
 * Note: If process(*) is ever called, closeWriter() should be called.
 * Otherwise, no need to call closeWriter().
 */
public class IntermediateForm implements Writable {

  private IndexUpdateConfiguration iconf = null;
  private final Collection<Term> deleteList;
  private RAMDirectory dir;
  private IndexWriter writer;
  private int numDocs;

  /**
   * Constructor
   * @throws IOException
   */
  public IntermediateForm() throws IOException {
    deleteList = new ConcurrentLinkedQueue<Term>();
    dir = new RAMDirectory();
    writer = null;
    numDocs = 0;
  }

  /**
   * Configure using an index update configuration.
   * @param iconf  the index update configuration
   */
  public void configure(IndexUpdateConfiguration iconf) {
    this.iconf = iconf;
  }

  /**
   * Get the ram directory of the intermediate form.
   * @return the ram directory
   */
  public Directory getDirectory() {
    return dir;
  }

  /**
   * Get an iterator for the delete terms in the intermediate form.
   * @return an iterator for the delete terms
   */
  public Iterator<Term> deleteTermIterator() {
    return deleteList.iterator();
  }

  /**
   * This method is used by the index update mapper and process a document
   * operation into the current intermediate form.
   * @param doc  input document operation
   * @param analyzer  the analyzer
   * @throws IOException
   */
  public void process(DocumentAndOp doc, Analyzer analyzer) throws IOException {
    if (doc.getOp() == DocumentAndOp.Op.DELETE
        || doc.getOp() == DocumentAndOp.Op.UPDATE) {
      deleteList.add(doc.getTerm());

    }

    if (doc.getOp() == DocumentAndOp.Op.INSERT
        || doc.getOp() == DocumentAndOp.Op.UPDATE) {

      if (writer == null) {
        // analyzer is null because we specify an analyzer with addDocument
        writer = createWriter();
      }

      writer.addDocument(doc.getDocument(), analyzer);
      numDocs++;
    }

  }

  /**
   * This method is used by the index update combiner and process an
   * intermediate form into the current intermediate form. More specifically,
   * the input intermediate forms are a single-document ram index and/or a
   * single delete term.
   * @param form  the input intermediate form
   * @throws IOException
   */
  public void process(IntermediateForm form) throws IOException {
    if (form.deleteList.size() > 0) {
      deleteList.addAll(form.deleteList);
    }

    if (form.dir.sizeInBytes() > 0) {
      if (writer == null) {
        writer = createWriter();
      }

      writer.addIndexesNoOptimize(new Directory[] { form.dir });
      numDocs++;
    }

  }

  /**
   * Close the Lucene index writer associated with the intermediate form,
   * if created. Do not close the ram directory. In fact, there is no need
   * to close a ram directory.
   * @throws IOException
   */
  public void closeWriter() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName());
    buffer.append("[numDocs=");
    buffer.append(numDocs);
    buffer.append(", numDeletes=");
    buffer.append(deleteList.size());
    if (deleteList.size() > 0) {
      buffer.append("(");
      Iterator<Term> iter = deleteTermIterator();
      while (iter.hasNext()) {
        buffer.append(iter.next());
        buffer.append(" ");
      }
      buffer.append(")");
    }
    buffer.append("]");
    return buffer.toString();
  }

  private IndexWriter createWriter() throws IOException {
    IndexWriter writer =
        new IndexWriter(dir, false, null,
            new KeepOnlyLastCommitDeletionPolicy());
    writer.setUseCompoundFile(false);

    if (iconf != null) {
      int maxFieldLength = iconf.getIndexMaxFieldLength();
      if (maxFieldLength > 0) {
        writer.setMaxFieldLength(maxFieldLength);
      }
    }

    return writer;
  }

  private void resetForm() throws IOException {
    deleteList.clear();
    if (dir.sizeInBytes() > 0) {
      // it's ok if we don't close a ram directory
      dir.close();
      // an alternative is to delete all the files and reuse the ram directory
      dir = new RAMDirectory();
    }
    assert (writer == null);
    numDocs = 0;
  }

  // ///////////////////////////////////
  // Writable
  // ///////////////////////////////////

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(deleteList.size());
    for (Term term : deleteList) {
      Text.writeString(out, term.field());
      Text.writeString(out, term.text());
    }

    String[] files = dir.list();
    RAMDirectoryUtil.writeRAMFiles(out, dir, files);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    resetForm();

    int numDeleteTerms = in.readInt();
    for (int i = 0; i < numDeleteTerms; i++) {
      String field = Text.readString(in);
      String text = Text.readString(in);
      deleteList.add(new Term(field, text));
    }

    RAMDirectoryUtil.readRAMFiles(in, dir);
  }

}
