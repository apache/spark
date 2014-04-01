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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.lucene.analysis.Analyzer;

/**
 * This class applies local analysis on a key-value pair and then convert the
 * result docid-operation pair to a shard-and-intermediate form pair.
 */
public class IndexUpdateMapper<K extends WritableComparable, V extends Writable>
    extends MapReduceBase implements Mapper<K, V, Shard, IntermediateForm> {
  static final Log LOG = LogFactory.getLog(IndexUpdateMapper.class);

  /**
   * Get the map output key class.
   * @return the map output key class
   */
  public static Class<? extends WritableComparable> getMapOutputKeyClass() {
    return Shard.class;
  }

  /**
   * Get the map output value class.
   * @return the map output value class
   */
  public static Class<? extends Writable> getMapOutputValueClass() {
    return IntermediateForm.class;
  }

  IndexUpdateConfiguration iconf;
  private Analyzer analyzer;
  private Shard[] shards;
  private IDistributionPolicy distributionPolicy;

  private ILocalAnalysis<K, V> localAnalysis;
  private DocumentID tmpKey;
  private DocumentAndOp tmpValue;

  private OutputCollector<DocumentID, DocumentAndOp> tmpCollector =
      new OutputCollector<DocumentID, DocumentAndOp>() {
        public void collect(DocumentID key, DocumentAndOp value)
            throws IOException {
          tmpKey = key;
          tmpValue = value;
        }
      };

  /**
   * Map a key-value pair to a shard-and-intermediate form pair. Internally,
   * the local analysis is first applied to map the key-value pair to a
   * document id-and-operation pair, then the docid-and-operation pair is
   * mapped to a shard-intermediate form pair. The intermediate form is of the
   * form of a single-document ram index and/or a single delete term.
   */
  public void map(K key, V value,
      OutputCollector<Shard, IntermediateForm> output, Reporter reporter)
      throws IOException {

    synchronized (this) {
      localAnalysis.map(key, value, tmpCollector, reporter);

      if (tmpKey != null && tmpValue != null) {
        DocumentAndOp doc = tmpValue;
        IntermediateForm form = new IntermediateForm();
        form.configure(iconf);
        form.process(doc, analyzer);
        form.closeWriter();

        if (doc.getOp() == DocumentAndOp.Op.INSERT) {
          int chosenShard = distributionPolicy.chooseShardForInsert(tmpKey);
          if (chosenShard >= 0) {
            // insert into one shard
            output.collect(shards[chosenShard], form);
          } else {
            throw new IOException("Chosen shard for insert must be >= 0");
          }

        } else if (doc.getOp() == DocumentAndOp.Op.DELETE) {
          int chosenShard = distributionPolicy.chooseShardForDelete(tmpKey);
          if (chosenShard >= 0) {
            // delete from one shard
            output.collect(shards[chosenShard], form);
          } else {
            // broadcast delete to all shards
            for (int i = 0; i < shards.length; i++) {
              output.collect(shards[i], form);
            }
          }

        } else { // UPDATE
          int insertToShard = distributionPolicy.chooseShardForInsert(tmpKey);
          int deleteFromShard =
              distributionPolicy.chooseShardForDelete(tmpKey);

          if (insertToShard >= 0) {
            if (insertToShard == deleteFromShard) {
              // update into one shard
              output.collect(shards[insertToShard], form);
            } else {
              // prepare a deletion form
              IntermediateForm deletionForm = new IntermediateForm();
              deletionForm.configure(iconf);
              deletionForm.process(new DocumentAndOp(DocumentAndOp.Op.DELETE,
                  doc.getTerm()), analyzer);
              deletionForm.closeWriter();

              if (deleteFromShard >= 0) {
                // delete from one shard
                output.collect(shards[deleteFromShard], deletionForm);
              } else {
                // broadcast delete to all shards
                for (int i = 0; i < shards.length; i++) {
                  output.collect(shards[i], deletionForm);
                }
              }

              // prepare an insertion form
              IntermediateForm insertionForm = new IntermediateForm();
              insertionForm.configure(iconf);
              insertionForm.process(new DocumentAndOp(DocumentAndOp.Op.INSERT,
                  doc.getDocument()), analyzer);
              insertionForm.closeWriter();

              // insert into one shard
              output.collect(shards[insertToShard], insertionForm);
            }
          } else {
            throw new IOException("Chosen shard for insert must be >= 0");
          }
        }
      }
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf job) {
    iconf = new IndexUpdateConfiguration(job);
    analyzer =
        (Analyzer) ReflectionUtils.newInstance(
            iconf.getDocumentAnalyzerClass(), job);

    localAnalysis =
        (ILocalAnalysis) ReflectionUtils.newInstance(
            iconf.getLocalAnalysisClass(), job);
    localAnalysis.configure(job);

    shards = Shard.getIndexShards(iconf);

    distributionPolicy =
        (IDistributionPolicy) ReflectionUtils.newInstance(
            iconf.getDistributionPolicyClass(), job);
    distributionPolicy.init(shards);

    LOG.info("sea.document.analyzer = " + analyzer.getClass().getName());
    LOG.info("sea.local.analysis = " + localAnalysis.getClass().getName());
    LOG.info(shards.length + " shards = " + iconf.getIndexShards());
    LOG.info("sea.distribution.policy = "
        + distributionPolicy.getClass().getName());
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.MapReduceBase#close()
   */
  public void close() throws IOException {
    localAnalysis.close();
  }

}
