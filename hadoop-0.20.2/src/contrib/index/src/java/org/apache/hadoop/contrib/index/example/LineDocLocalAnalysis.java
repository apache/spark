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

package org.apache.hadoop.contrib.index.example;

import java.io.IOException;

import org.apache.hadoop.contrib.index.mapred.DocumentAndOp;
import org.apache.hadoop.contrib.index.mapred.DocumentID;
import org.apache.hadoop.contrib.index.mapred.ILocalAnalysis;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;

/**
 * Convert LineDocTextAndOp to DocumentAndOp as required by ILocalAnalysis.
 */
public class LineDocLocalAnalysis implements
    ILocalAnalysis<DocumentID, LineDocTextAndOp> {

  private static String docidFieldName = "id";
  private static String contentFieldName = "content";

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  public void map(DocumentID key, LineDocTextAndOp value,
      OutputCollector<DocumentID, DocumentAndOp> output, Reporter reporter)
      throws IOException {

    DocumentAndOp.Op op = value.getOp();
    Document doc = null;
    Term term = null;

    if (op == DocumentAndOp.Op.INSERT || op == DocumentAndOp.Op.UPDATE) {
      doc = new Document();
      doc.add(new Field(docidFieldName, key.getText().toString(),
          Field.Store.YES, Field.Index.UN_TOKENIZED));
      doc.add(new Field(contentFieldName, value.getText().toString(),
          Field.Store.NO, Field.Index.TOKENIZED));
    }

    if (op == DocumentAndOp.Op.DELETE || op == DocumentAndOp.Op.UPDATE) {
      term = new Term(docidFieldName, key.getText().toString());
    }

    output.collect(key, new DocumentAndOp(op, doc, term));
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.JobConfigurable#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf job) {
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Closeable#close()
   */
  public void close() throws IOException {
  }

}
