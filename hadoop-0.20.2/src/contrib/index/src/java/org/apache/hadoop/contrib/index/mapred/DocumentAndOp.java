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

import org.apache.hadoop.io.Writable;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

/**
 * This class represents an indexing operation. The operation can be an insert,
 * a delete or an update. If the operation is an insert or an update, a (new)
 * document must be specified. If the operation is a delete or an update, a
 * delete term must be specified.
 */
public class DocumentAndOp implements Writable {

  /**
   * This class represents the type of an operation - an insert, a delete or
   * an update.
   */
  public static final class Op {
    public static final Op INSERT = new Op("INSERT");
    public static final Op DELETE = new Op("DELETE");
    public static final Op UPDATE = new Op("UPDATE");

    private String name;

    private Op(String name) {
      this.name = name;
    }

    public String toString() {
      return name;
    }
  }

  private Op op;
  private Document doc;
  private Term term;

  /**
   * Constructor for no operation.
   */
  public DocumentAndOp() {
  }

  /**
   * Constructor for an insert operation.
   * @param op
   * @param doc
   */
  public DocumentAndOp(Op op, Document doc) {
    assert (op == Op.INSERT);
    this.op = op;
    this.doc = doc;
    this.term = null;
  }

  /**
   * Constructor for a delete operation.
   * @param op
   * @param term
   */
  public DocumentAndOp(Op op, Term term) {
    assert (op == Op.DELETE);
    this.op = op;
    this.doc = null;
    this.term = term;
  }

  /**
   * Constructor for an insert, a delete or an update operation.
   * @param op
   * @param doc
   * @param term
   */
  public DocumentAndOp(Op op, Document doc, Term term) {
    if (op == Op.INSERT) {
      assert (doc != null);
      assert (term == null);
    } else if (op == Op.DELETE) {
      assert (doc == null);
      assert (term != null);
    } else {
      assert (op == Op.UPDATE);
      assert (doc != null);
      assert (term != null);
    }
    this.op = op;
    this.doc = doc;
    this.term = term;
  }

  /**
   * Set the instance to be an insert operation.
   * @param doc
   */
  public void setInsert(Document doc) {
    this.op = Op.INSERT;
    this.doc = doc;
    this.term = null;
  }

  /**
   * Set the instance to be a delete operation.
   * @param term
   */
  public void setDelete(Term term) {
    this.op = Op.DELETE;
    this.doc = null;
    this.term = term;
  }

  /**
   * Set the instance to be an update operation.
   * @param doc
   * @param term
   */
  public void setUpdate(Document doc, Term term) {
    this.op = Op.UPDATE;
    this.doc = doc;
    this.term = term;
  }

  /**
   * Get the type of operation.
   * @return the type of the operation.
   */
  public Op getOp() {
    return op;
  }

  /**
   * Get the document.
   * @return the document
   */
  public Document getDocument() {
    return doc;
  }

  /**
   * Get the term.
   * @return the term
   */
  public Term getTerm() {
    return term;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getName());
    buffer.append("[op=");
    buffer.append(op);
    buffer.append(", doc=");
    if (doc != null) {
      buffer.append(doc);
    } else {
      buffer.append("null");
    }
    buffer.append(", term=");
    if (term != null) {
      buffer.append(term);
    } else {
      buffer.append("null");
    }
    buffer.append("]");
    return buffer.toString();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    throw new IOException(this.getClass().getName()
        + ".write should never be called");
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    throw new IOException(this.getClass().getName()
        + ".readFields should never be called");
  }
}
